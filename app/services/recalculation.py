"""历史作业重算的进程内作业编排。

设计要点：

- 创建作业时冻结评分策略参数与筛选范围：策略参数来自不可变的策略版本记录，
  筛选结果在创建时物化为工作单元，之后发布新策略或新增作业都不影响在跑作业。
- 工作单元按 operation_id 稳定顺序领取；领取通过带条件的 UPDATE 占位，
  并发领取与领取超时后的重复领取都不会让同一项被二次写入（claim_token 轮换）。
- 暂停只阻止继续领取，已领取批次仍可回报；取消后在途批次的迟到回报一律拒绝，
  已完成进度始终持久保留。
- 进度计数器、工作单元状态和批次结果都在业务数据库中，进程重启后新编排器
  直接从检查点继续；超过租约的领取会被其他批次重新接管。
- 时钟（now）和租约时长可注入，便于稳定验证时间相关行为。
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Callable, Optional, Sequence

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.models import (
    Annotation,
    OperationData,
    QualityPolicyRecord,
    RecalculationBatchRecord,
    RecalculationJobRecord,
    RecalculationOutcomeRecord,
    RecalculationUnitRecord,
)
from app.services.scoring import compute_operation_quality

# 作业与单元状态
STATUS_RUNNING = "running"
STATUS_PAUSED = "paused"
STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"
STATUS_FAILED = "failed"
TERMINAL_STATUSES = {STATUS_COMPLETED, STATUS_CANCELLED, STATUS_FAILED}

UNIT_PENDING = "pending"
UNIT_CLAIMED = "claimed"
UNIT_SUCCEEDED = "succeeded"
UNIT_SKIPPED = "skipped"
UNIT_FAILED = "failed"

RESULT_SUCCESS = "success"
RESULT_SKIPPED = "skipped"
RESULT_FAILED = "failed"

DEFAULT_LEASE_TIMEOUT = timedelta(minutes=30)

SCOPE_FILTER_FIELDS = (
    "robot_model_id",
    "scene_id",
    "skill_id",
    "robot_serial",
    "data_grade",
)


class RecalculationError(Exception):
    """重算编排相关错误的基类。"""


class JobNotFoundError(RecalculationError, LookupError):
    """作业不存在。"""


class JobStateError(RecalculationError):
    """作业当前状态不允许该操作。"""


class PolicyStaleError(RecalculationError):
    """作业冻结的策略版本已失效。"""


class ClaimLostError(RecalculationError):
    """批次领取已失效（重复回报、租约过期后被接管），本次写入被拒绝。"""


class EmptyScopeError(RecalculationError):
    """筛选范围内没有任何可重算的作业。"""


@dataclass(frozen=True)
class FrozenPolicy:
    """创建作业时冻结下来的评分参数。"""

    revision: int
    name: str
    completeness_weight: float
    annotation_weight: float
    thresholds: dict[str, float]
    fingerprint: str

    def as_dict(self) -> dict:
        return {
            "revision": self.revision,
            "name": self.name,
            "completeness_weight": self.completeness_weight,
            "annotation_weight": self.annotation_weight,
            "grade_a_threshold": self.thresholds["grade_a"],
            "grade_b_threshold": self.thresholds["grade_b"],
            "grade_c_threshold": self.thresholds["grade_c"],
            "fingerprint": self.fingerprint,
        }


@dataclass(frozen=True)
class Claim:
    """一次成功领取的批次及其工作单元。"""

    job_id: int
    batch_id: int
    sequence: int
    token: str
    operation_ids: list[int]
    claimed_by: Optional[str]


@dataclass(frozen=True)
class UnitOutcome:
    """单项处理结果，由处理器计算后交给编排器统一落库。"""

    operation_id: int
    result: str
    reason: Optional[str] = None
    completeness_score: Optional[float] = None
    quality_score: Optional[float] = None
    before_grade: Optional[str] = None
    after_grade: Optional[str] = None

    @classmethod
    def success(cls, operation_id: int, scores, before_grade: Optional[str]) -> "UnitOutcome":
        return cls(
            operation_id=operation_id,
            result=RESULT_SUCCESS,
            completeness_score=scores.completeness_score,
            quality_score=scores.quality_score,
            before_grade=before_grade,
            after_grade=scores.data_grade,
        )

    @classmethod
    def skipped(cls, operation_id: int, reason: str) -> "UnitOutcome":
        return cls(operation_id=operation_id, result=RESULT_SKIPPED, reason=reason)

    @classmethod
    def failed(cls, operation_id: int, reason: str) -> "UnitOutcome":
        return cls(operation_id=operation_id, result=RESULT_FAILED, reason=reason)


@dataclass
class BatchRunResult:
    """领取并处理一个批次后的结果视图。"""

    claim: Claim
    outcomes: list[UnitOutcome]
    finished: bool

    @property
    def success_count(self) -> int:
        return sum(1 for item in self.outcomes if item.result == RESULT_SUCCESS)

    @property
    def skipped_count(self) -> int:
        return sum(1 for item in self.outcomes if item.result == RESULT_SKIPPED)

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.outcomes if item.result == RESULT_FAILED)


UnitProcessor = Callable[[Session, FrozenPolicy, int], UnitOutcome]


def policy_fingerprint(
    name: str,
    completeness_weight: float,
    annotation_weight: float,
    thresholds: dict[str, float],
) -> str:
    payload = "|".join(
        [
            name.strip(),
            repr(round(float(completeness_weight), 6)),
            repr(round(float(annotation_weight), 6)),
            repr(round(float(thresholds["grade_a"]), 6)),
            repr(round(float(thresholds["grade_b"]), 6)),
            repr(round(float(thresholds["grade_c"]), 6)),
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def validate_policy_params(
    completeness_weight: float,
    annotation_weight: float,
    thresholds: dict[str, float],
) -> None:
    weights = (float(completeness_weight), float(annotation_weight))
    if min(weights) < 0 or max(weights) > 1:
        raise ValueError("完整度权重和标注质量权重必须位于零到一之间")
    if abs(sum(weights) - 1.0) > 1e-6:
        raise ValueError("完整度权重和标注质量权重之和必须为1.0")
    a, b, c = (
        float(thresholds["grade_a"]),
        float(thresholds["grade_b"]),
        float(thresholds["grade_c"]),
    )
    if not (1.0 >= a >= b >= c >= 0.0):
        raise ValueError("分级阈值必须满足 A >= B >= C 且位于零到一之间")


def default_processor(db: Session, policy: FrozenPolicy, operation_id: int) -> UnitOutcome:
    """默认处理器：用冻结策略对单条作业重新评分；作业缺失则跳过。"""

    operation = db.query(OperationData).filter(OperationData.id == operation_id).first()
    if operation is None:
        return UnitOutcome.skipped(operation_id, "作业记录已不存在，跳过")
    before_grade = operation.data_grade
    annotation = (
        db.query(Annotation)
        .filter(Annotation.operation_data_id == operation_id)
        .first()
    )
    scores = compute_operation_quality(
        operation=operation,
        annotation=annotation,
        completeness_weight=policy.completeness_weight,
        annotation_weight=policy.annotation_weight,
        thresholds=policy.thresholds,
    )
    return UnitOutcome.success(operation_id, scores, before_grade)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _naive_utc(value: datetime) -> datetime:
    """SQLite 以朴素 UTC 存储，所有写入时间统一归一化后再比较。"""
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


class RecalculationOrchestrator:
    """进程内的重算作业编排器，所有状态以业务数据库为准。"""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        now: Optional[Callable[[], datetime]] = None,
        lease_timeout: timedelta = DEFAULT_LEASE_TIMEOUT,
        processor: UnitProcessor = default_processor,
    ) -> None:
        self._session_factory = session_factory
        self._now = now or _utc_now
        self._lease_timeout = lease_timeout
        self._processor = processor

    # ------------------------------------------------------------------ 策略

    def publish_policy(
        self,
        *,
        name: str,
        completeness_weight: float = 0.5,
        annotation_weight: float = 0.5,
        grade_a_threshold: float = 0.9,
        grade_b_threshold: float = 0.7,
        grade_c_threshold: float = 0.5,
        revision: Optional[int] = None,
    ) -> FrozenPolicy:
        """发布一个新的策略版本。历史版本保持有效，在跑作业继续按冻结版本执行。"""

        name = name.strip()
        if not name:
            raise ValueError("策略名称不能为空")
        thresholds = {
            "grade_a": float(grade_a_threshold),
            "grade_b": float(grade_b_threshold),
            "grade_c": float(grade_c_threshold),
        }
        validate_policy_params(
            float(completeness_weight), float(annotation_weight), thresholds
        )
        fingerprint = policy_fingerprint(
            name, float(completeness_weight), float(annotation_weight), thresholds
        )

        db = self._session_factory()
        try:
            if revision is None:
                last = (
                    db.query(QualityPolicyRecord)
                    .order_by(QualityPolicyRecord.revision.desc())
                    .first()
                )
                revision = 1 if last is None else last.revision + 1
            elif (
                db.query(QualityPolicyRecord)
                .filter(QualityPolicyRecord.revision == revision)
                .first()
            ):
                raise ValueError(f"策略版本 {revision} 已存在")

            record = QualityPolicyRecord(
                revision=revision,
                name=name,
                completeness_weight=float(completeness_weight),
                annotation_weight=float(annotation_weight),
                grade_a_threshold=thresholds["grade_a"],
                grade_b_threshold=thresholds["grade_b"],
                grade_c_threshold=thresholds["grade_c"],
                fingerprint=fingerprint,
                is_active=True,
                created_at=_naive_utc(self._now()),
            )
            db.add(record)
            db.commit()
            return self._freeze(record)
        finally:
            db.close()

    def deactivate_policy(self, revision: int) -> None:
        """显式作废某个策略版本；引用它的作业在下次领取/继续时失败停止。"""

        db = self._session_factory()
        try:
            record = self._get_policy(db, revision)
            record.is_active = False
            db.commit()
        finally:
            db.close()

    def list_policies(self) -> list[dict]:
        db = self._session_factory()
        try:
            records = (
                db.query(QualityPolicyRecord)
                .order_by(QualityPolicyRecord.revision.asc())
                .all()
            )
            return [
                {
                    **self._freeze(record).as_dict(),
                    "is_active": record.is_active,
                    "created_at": record.created_at,
                }
                for record in records
            ]
        finally:
            db.close()

    # ------------------------------------------------------------------ 作业

    def create_job(
        self,
        *,
        scope: Optional[dict] = None,
        policy_revision: Optional[int] = None,
        created_by: Optional[str] = None,
    ) -> dict:
        """创建重算作业：冻结策略版本，并把筛选范围内的作业物化为有序工作单元。"""

        filters = self._normalize_scope(scope or {})
        db = self._session_factory()
        try:
            policy_record = self._resolve_policy(db, policy_revision)

            query = db.query(OperationData.id)
            if filters.get("robot_model_id") is not None:
                query = query.filter(
                    OperationData.robot_model_id == filters["robot_model_id"]
                )
            if filters.get("scene_id") is not None:
                query = query.filter(OperationData.scene_id == filters["scene_id"])
            if filters.get("skill_id") is not None:
                query = query.filter(OperationData.skill_id == filters["skill_id"])
            if filters.get("robot_serial"):
                query = query.filter(
                    OperationData.robot_serial == filters["robot_serial"]
                )
            if filters.get("data_grade"):
                query = query.filter(OperationData.data_grade == filters["data_grade"])
            operation_ids = [row[0] for row in query.order_by(OperationData.id.asc()).all()]
            if not operation_ids:
                raise EmptyScopeError("筛选范围内没有可重算的作业")

            now = _naive_utc(self._now())
            job = RecalculationJobRecord(
                policy_revision=policy_record.id,
                status=STATUS_RUNNING,
                total_count=len(operation_ids),
                scope={"filters": filters},
                created_by=created_by,
                created_at=now,
                started_at=now,
            )
            db.add(job)
            db.flush()
            db.add_all(
                RecalculationUnitRecord(
                    job_id=job.id,
                    operation_id=operation_id,
                    status=UNIT_PENDING,
                )
                for operation_id in operation_ids
            )
            db.commit()
            job_id = job.id
        finally:
            db.close()

        return self.get_job(job_id)

    def pause_job(self, job_id: int) -> dict:
        db = self._session_factory()
        try:
            job = self._get_job(db, job_id)
            if job.status == STATUS_PAUSED:
                return self.get_job(job_id)
            if job.status != STATUS_RUNNING:
                raise JobStateError(f"作业当前状态为 {job.status}，无法暂停")
            job.status = STATUS_PAUSED
            job.paused_at = _naive_utc(self._now())
            db.commit()
        finally:
            db.close()
        return self.get_job(job_id)

    def resume_job(self, job_id: int) -> dict:
        db = self._session_factory()
        try:
            job = self._get_job(db, job_id)
            if job.status == STATUS_RUNNING:
                return self.get_job(job_id)
            if job.status != STATUS_PAUSED:
                raise JobStateError(f"作业当前状态为 {job.status}，无法继续")
            self._ensure_policy_active(db, job)
            job.status = STATUS_RUNNING
            job.resumed_at = _naive_utc(self._now())
            # 暂停期间最后一个在途批次可能已把全部单元处理完
            finished = (
                job.succeeded_count + job.skipped_count + job.failed_count
            )
            if finished >= job.total_count:
                job.status = STATUS_COMPLETED
                job.completed_at = job.resumed_at
            db.commit()
        finally:
            db.close()
        return self.get_job(job_id)

    def cancel_job(self, job_id: int) -> dict:
        db = self._session_factory()
        try:
            job = self._get_job(db, job_id)
            if job.status == STATUS_CANCELLED:
                return self.get_job(job_id)
            if job.status in TERMINAL_STATUSES:
                raise JobStateError(f"作业已结束（{job.status}），无法取消")
            job.status = STATUS_CANCELLED
            job.cancelled_at = _naive_utc(self._now())
            db.commit()
        finally:
            db.close()
        return self.get_job(job_id)

    def get_job(self, job_id: int) -> dict:
        db = self._session_factory()
        try:
            job = self._get_job(db, job_id)
            return self._job_view(db, job)
        finally:
            db.close()

    def list_jobs(self, status: Optional[str] = None) -> list[dict]:
        db = self._session_factory()
        try:
            query = db.query(RecalculationJobRecord)
            if status:
                query = query.filter(RecalculationJobRecord.status == status)
            jobs = query.order_by(RecalculationJobRecord.id.desc()).all()
            return [self._job_view(db, job) for job in jobs]
        finally:
            db.close()

    # ------------------------------------------------------------------ 领取

    def claim_batch(
        self,
        job_id: int,
        batch_size: int = 50,
        claimed_by: Optional[str] = None,
    ) -> Claim:
        """按稳定顺序领取一个批次。返回空 Claim 表示暂无可领取单元。"""

        last_error: Optional[Exception] = None
        for attempt in range(5):
            try:
                return self._claim_once(job_id, batch_size, claimed_by)
            except OperationalError as exc:  # SQLite 写锁竞争，退避后整单重试
                last_error = exc
                time.sleep(0.02 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def _claim_once(
        self,
        job_id: int,
        batch_size: int = 50,
        claimed_by: Optional[str] = None,
    ) -> Claim:
        if batch_size < 1:
            raise ValueError("批次大小必须大于零")

        db = self._session_factory()
        try:
            # 单元占位与批次序号都可能在并发下撞车，撞了就整轮重试。
            for _ in range(10):
                job = self._get_job(db, job_id)
                if job.status in TERMINAL_STATUSES:
                    return self._empty_claim(job_id)
                if job.status == STATUS_PAUSED:
                    raise JobStateError("作业已暂停，无法领取新批次")
                self._ensure_policy_active(db, job)

                cutoff = self._lease_cutoff()
                available = or_(
                    RecalculationUnitRecord.status == UNIT_PENDING,
                    (
                        (RecalculationUnitRecord.status == UNIT_CLAIMED)
                        & (RecalculationUnitRecord.claimed_at < cutoff)
                    ),
                )
                candidates = (
                    db.query(RecalculationUnitRecord)
                    .filter(RecalculationUnitRecord.job_id == job_id, available)
                    .order_by(RecalculationUnitRecord.operation_id.asc())
                    .limit(batch_size)
                    .all()
                )
                if not candidates:
                    return self._empty_claim(job_id)

                # 找到候选后在作业行上自增批次序号：该写入会拿到作业行锁，
                # 同一作业的并发领取在此串行化，序号天然唯一。
                db.query(RecalculationJobRecord).filter(
                    RecalculationJobRecord.id == job_id
                ).update(
                    {
                        RecalculationJobRecord.next_batch_sequence:
                            RecalculationJobRecord.next_batch_sequence + 1
                    },
                    synchronize_session=False,
                )
                db.flush()
                db.refresh(job)
                sequence = job.next_batch_sequence

                unit_ids = [unit.id for unit in candidates]
                expired_batch_ids = {
                    unit.batch_id
                    for unit in candidates
                    if unit.status == UNIT_CLAIMED and unit.batch_id is not None
                }

                now = self._now()
                token = uuid.uuid4().hex
                batch = RecalculationBatchRecord(
                    job_id=job_id,
                    sequence=sequence,
                    status="running",
                    claim_token=token,
                    claimed_by=claimed_by,
                    created_at=_naive_utc(now),
                )
                db.add(batch)
                db.flush()

                # 条件更新：只有仍可领取（待处理或租约过期）的单元才会被占位，
                # 与其他领取者竞争时拿不到的行保持原状，杜绝重复领取。
                taken = (
                    db.query(RecalculationUnitRecord)
                    .filter(
                        RecalculationUnitRecord.id.in_(unit_ids),
                        RecalculationUnitRecord.job_id == job_id,
                        available,
                    )
                    .update(
                        {
                            RecalculationUnitRecord.status: UNIT_CLAIMED,
                            RecalculationUnitRecord.batch_id: batch.id,
                            RecalculationUnitRecord.claimed_by: claimed_by,
                            RecalculationUnitRecord.claimed_at: _naive_utc(now),
                            RecalculationUnitRecord.claim_token: token,
                        },
                        synchronize_session=False,
                    )
                )
                if taken == 0:
                    db.rollback()
                    continue

                for old_batch_id in expired_batch_ids:
                    old_batch = db.get(RecalculationBatchRecord, old_batch_id)
                    if old_batch is not None and old_batch.status == "running":
                        old_batch.status = "expired"
                        old_batch.finished_at = _naive_utc(now)

                # claimed_count 以当前处于领取态的实际行数为准，
                # 租约过期重领不会造成计数虚高。子查询与序号自增在同一事务原子提交，
                # 并发领取之间也不会互相覆盖。
                claimed_subquery = (
                    select(func.count())
                    .select_from(RecalculationUnitRecord)
                    .where(
                        RecalculationUnitRecord.job_id == job_id,
                        RecalculationUnitRecord.status == UNIT_CLAIMED,
                    )
                    .scalar_subquery()
                )
                db.query(RecalculationJobRecord).filter(
                    RecalculationJobRecord.id == job_id
                ).update(
                    {RecalculationJobRecord.claimed_count: claimed_subquery},
                    synchronize_session=False,
                )
                try:
                    db.commit()
                except (IntegrityError, OperationalError):
                    # 并发写入冲突，整轮重试。
                    db.rollback()
                    continue

                claimed_units = (
                    db.query(RecalculationUnitRecord)
                    .filter(
                        RecalculationUnitRecord.batch_id == batch.id,
                        RecalculationUnitRecord.claim_token == token,
                    )
                    .order_by(RecalculationUnitRecord.operation_id.asc())
                    .all()
                )
                return Claim(
                    job_id=job_id,
                    batch_id=batch.id,
                    sequence=sequence,
                    token=token,
                    operation_ids=[unit.operation_id for unit in claimed_units],
                    claimed_by=claimed_by,
                )
            return self._empty_claim(job_id)
        finally:
            db.close()

    def report_batch(
        self,
        job_id: int,
        batch_id: int,
        token: str,
        outcomes: Sequence[UnitOutcome],
    ) -> dict:
        """回报批次结果。领取凭证失效（重复回报/被接管/作业取消）时整批拒绝写入。"""

        last_error: Optional[Exception] = None
        for attempt in range(5):
            try:
                return self._report_once(job_id, batch_id, token, outcomes)
            except OperationalError as exc:  # SQLite 写锁竞争，退避后整单重试
                last_error = exc
                time.sleep(0.02 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def _report_once(
        self,
        job_id: int,
        batch_id: int,
        token: str,
        outcomes: Sequence[UnitOutcome],
    ) -> dict:
        db = self._session_factory()
        try:
            batch = (
                db.query(RecalculationBatchRecord)
                .filter(
                    RecalculationBatchRecord.id == batch_id,
                    RecalculationBatchRecord.job_id == job_id,
                )
                .first()
            )
            if batch is None:
                raise ClaimLostError("批次不存在或不属于该作业")
            job = self._get_job(db, job_id)

            if job.status == STATUS_CANCELLED:
                raise JobStateError("作业已取消，拒绝迟到的批次回报")
            if job.status == STATUS_FAILED:
                raise JobStateError("作业已失败，拒绝迟到的批次回报")
            if batch.status != "running":
                raise ClaimLostError(f"批次状态为 {batch.status}，不能重复回报")
            if batch.claim_token != token:
                raise ClaimLostError("领取凭证已失效（可能被其他执行者接管）")

            units = (
                db.query(RecalculationUnitRecord)
                .filter(RecalculationUnitRecord.batch_id == batch_id)
                .order_by(RecalculationUnitRecord.operation_id.asc())
                .all()
            )
            unit_by_operation = {unit.operation_id: unit for unit in units}
            outcome_by_operation = {item.operation_id: item for item in outcomes}
            if set(unit_by_operation) != set(outcome_by_operation):
                raise ClaimLostError("回报的作业集合与领取的工作单元不一致")

            # 立即在作业行上取写锁并做条件校验：取消若先于本回报落库，
            # 这里条件不成立，整批拒绝；若本回报先取到写锁，取消只能等待，
            # 等待结束后取消的是已完成批次，顺序天然安全。
            locked = (
                db.query(RecalculationJobRecord)
                .filter(
                    RecalculationJobRecord.id == job_id,
                    RecalculationJobRecord.status.in_([STATUS_RUNNING, STATUS_PAUSED]),
                )
                .update(
                    {RecalculationJobRecord.last_processed_id: RecalculationJobRecord.last_processed_id},
                    synchronize_session=False,
                )
            )
            db.flush()
            if locked == 0:
                db.rollback()
                current = self._get_job(self._session_factory(), job_id)
                if current.status == STATUS_CANCELLED:
                    raise JobStateError("作业已取消，拒绝迟到的批次回报")
                if current.status == STATUS_FAILED:
                    raise JobStateError("作业已失败，拒绝迟到的批次回报")
                raise JobStateError(f"作业状态为 {current.status}，拒绝批次回报")

            now = self._now()
            success = skipped = failed = 0
            last_processed_id = job.last_processed_id
            for operation_id in sorted(outcome_by_operation):
                outcome = outcome_by_operation[operation_id]
                unit = unit_by_operation[operation_id]

                # 凭信条件更新：重复回报或租约接管后的旧回报都改不动这行。
                updated = (
                    db.query(RecalculationUnitRecord)
                    .filter(
                        RecalculationUnitRecord.id == unit.id,
                        RecalculationUnitRecord.status == UNIT_CLAIMED,
                        RecalculationUnitRecord.claim_token == token,
                    )
                    .update(
                        {
                            RecalculationUnitRecord.status: outcome.result,
                            RecalculationUnitRecord.finished_at: _naive_utc(now),
                        },
                        synchronize_session=False,
                    )
                )
                if updated != 1:
                    db.rollback()
                    raise ClaimLostError(
                        f"作业 {operation_id} 的领取凭证已失效，整批拒绝写入"
                    )

                if outcome.result == RESULT_SUCCESS:
                    operation = (
                        db.query(OperationData)
                        .filter(OperationData.id == operation_id)
                        .first()
                    )
                    if operation is None:
                        # 作业在处理期间被删除：记为跳过而不是写入失败。
                        unit.status = UNIT_SKIPPED
                        skipped += 1
                        db.add(
                            RecalculationOutcomeRecord(
                                batch_id=batch_id,
                                operation_id=operation_id,
                                result=RESULT_SKIPPED,
                                reason="作业记录已不存在，跳过",
                                before_grade=outcome.before_grade,
                                after_grade=None,
                                recorded_at=_naive_utc(now),
                            )
                        )
                    else:
                        operation.completeness_score = outcome.completeness_score
                        operation.quality_score = outcome.quality_score
                        operation.data_grade = outcome.after_grade
                        success += 1
                        db.add(
                            RecalculationOutcomeRecord(
                                batch_id=batch_id,
                                operation_id=operation_id,
                                result=RESULT_SUCCESS,
                                reason=outcome.reason,
                                before_grade=outcome.before_grade,
                                after_grade=outcome.after_grade,
                                recorded_at=_naive_utc(now),
                            )
                        )
                elif outcome.result == RESULT_SKIPPED:
                    skipped += 1
                    db.add(
                        RecalculationOutcomeRecord(
                            batch_id=batch_id,
                            operation_id=operation_id,
                            result=RESULT_SKIPPED,
                            reason=outcome.reason,
                            recorded_at=_naive_utc(now),
                        )
                    )
                else:
                    failed += 1
                    db.add(
                        RecalculationOutcomeRecord(
                            batch_id=batch_id,
                            operation_id=operation_id,
                            result=RESULT_FAILED,
                            reason=outcome.reason,
                            before_grade=outcome.before_grade,
                            after_grade=outcome.after_grade,
                            recorded_at=_naive_utc(now),
                        )
                    )
                last_processed_id = (
                    operation_id
                    if last_processed_id is None
                    else max(last_processed_id, operation_id)
                )

            finished_at = _naive_utc(now)
            batch.status = "completed"
            batch.finished_at = finished_at
            batch.outcome_count = len(outcomes)
            batch.success_count = success
            batch.skipped_count = skipped
            batch.failed_count = failed

            # 计数用原子表达式更新，避免并发回报互相覆盖（丢失更新）。
            claimed_subquery = (
                select(func.count())
                .select_from(RecalculationUnitRecord)
                .where(
                    RecalculationUnitRecord.job_id == job_id,
                    RecalculationUnitRecord.status == UNIT_CLAIMED,
                )
                .scalar_subquery()
            )
            db.query(RecalculationJobRecord).filter(
                RecalculationJobRecord.id == job_id
            ).update(
                {
                    RecalculationJobRecord.succeeded_count:
                        RecalculationJobRecord.succeeded_count + success,
                    RecalculationJobRecord.skipped_count:
                        RecalculationJobRecord.skipped_count + skipped,
                    RecalculationJobRecord.failed_count:
                        RecalculationJobRecord.failed_count + failed,
                    RecalculationJobRecord.claimed_count: claimed_subquery,
                    RecalculationJobRecord.last_processed_id: func.max(
                        RecalculationJobRecord.last_processed_id, last_processed_id
                    ),
                },
                synchronize_session=False,
            )

            # 是否完成以提交后的真实单元状态为准，避免并发下误判/漏判。
            in_flight = (
                db.query(RecalculationUnitRecord)
                .filter(
                    RecalculationUnitRecord.job_id == job_id,
                    RecalculationUnitRecord.status.in_(
                        [UNIT_PENDING, UNIT_CLAIMED]
                    ),
                )
                .count()
            )
            if in_flight == 0:
                db.query(RecalculationJobRecord).filter(
                    RecalculationJobRecord.id == job_id,
                    RecalculationJobRecord.status == STATUS_RUNNING,
                ).update(
                    {
                        RecalculationJobRecord.status: STATUS_COMPLETED,
                        RecalculationJobRecord.completed_at: finished_at,
                    },
                    synchronize_session=False,
                )

            db.commit()
            db.refresh(job)
            return {
                "batch": self._batch_view(batch),
                "job_status": job.status,
                "job": self._job_view(db, job),
            }
        finally:
            db.close()

    # ----------------------------------------------------------- 领取并处理

    def run_batch(
        self,
        job_id: int,
        batch_size: int = 50,
        claimed_by: Optional[str] = None,
        *,
        processor: Optional[UnitProcessor] = None,
    ) -> Optional[BatchRunResult]:
        """领取一个批次并用处理器逐项计算，单项异常隔离为失败结果后统一回报。"""

        process = processor or self._processor
        claim = self.claim_batch(job_id, batch_size, claimed_by)
        if not claim.operation_ids:
            return None

        frozen = self._load_frozen_policy(job_id)
        outcomes: list[UnitOutcome] = []
        db = self._session_factory()
        try:
            for operation_id in claim.operation_ids:
                try:
                    outcome = process(db, frozen, operation_id)
                except Exception as exc:  # 单项失败隔离，不影响同批其他作业
                    db.rollback()
                    outcome = UnitOutcome.failed(operation_id, f"{type(exc).__name__}: {exc}")
                outcomes.append(outcome)
        finally:
            db.close()

        batch_view = self.report_batch(job_id, claim.batch_id, claim.token, outcomes)
        return BatchRunResult(
            claim=claim,
            outcomes=outcomes,
            finished=batch_view["job_status"] == STATUS_COMPLETED,
        )

    def run_until_done(
        self,
        job_id: int,
        batch_size: int = 50,
        claimed_by: Optional[str] = None,
        *,
        processor: Optional[UnitProcessor] = None,
    ) -> list[BatchRunResult]:
        """循环领取处理直到当前没有可领取单元（测试与低峰串行回填使用）。"""

        results: list[BatchRunResult] = []
        while True:
            result = self.run_batch(job_id, batch_size, claimed_by, processor=processor)
            if result is None:
                return results
            results.append(result)

    # ------------------------------------------------------------------ 查询

    def list_batches(self, job_id: int) -> list[dict]:
        db = self._session_factory()
        try:
            self._get_job(db, job_id)
            batches = (
                db.query(RecalculationBatchRecord)
                .filter(RecalculationBatchRecord.job_id == job_id)
                .order_by(RecalculationBatchRecord.sequence.asc())
                .all()
            )
            return [self._batch_view(batch) for batch in batches]
        finally:
            db.close()

    def list_outcomes(
        self,
        job_id: int,
        *,
        batch_id: Optional[int] = None,
        result: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]:
        db = self._session_factory()
        try:
            self._get_job(db, job_id)
            query = (
                db.query(RecalculationOutcomeRecord)
                .join(
                    RecalculationBatchRecord,
                    RecalculationOutcomeRecord.batch_id == RecalculationBatchRecord.id,
                )
                .filter(RecalculationBatchRecord.job_id == job_id)
            )
            if batch_id is not None:
                query = query.filter(RecalculationBatchRecord.id == batch_id)
            if result:
                query = query.filter(RecalculationOutcomeRecord.result == result)
            rows = (
                query.order_by(
                    RecalculationBatchRecord.sequence.asc(),
                    RecalculationOutcomeRecord.operation_id.asc(),
                )
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [
                {
                    "batch_id": row.batch_id,
                    "operation_id": row.operation_id,
                    "result": row.result,
                    "reason": row.reason,
                    "before_grade": row.before_grade,
                    "after_grade": row.after_grade,
                    "recorded_at": row.recorded_at,
                }
                for row in rows
            ]
        finally:
            db.close()

    # ------------------------------------------------------------------ 内部

    def _load_frozen_policy(self, job_id: int) -> FrozenPolicy:
        db = self._session_factory()
        try:
            job = self._get_job(db, job_id)
            return self._freeze(job.policy)
        finally:
            db.close()

    @staticmethod
    def _normalize_scope(scope: dict) -> dict:
        filters: dict = {}
        for field_name in SCOPE_FILTER_FIELDS:
            value = scope.get(field_name)
            if value is not None:
                filters[field_name] = value
        return filters

    def _resolve_policy(
        self, db: Session, revision: Optional[int]
    ) -> QualityPolicyRecord:
        if revision is not None:
            record = self._get_policy(db, revision)
            if not record.is_active:
                raise PolicyStaleError(f"策略版本 {revision} 已失效，不能创建作业")
            return record
        record = (
            db.query(QualityPolicyRecord)
            .filter(QualityPolicyRecord.is_active.is_(True))
            .order_by(QualityPolicyRecord.revision.desc())
            .first()
        )
        if record is None:
            raise PolicyStaleError("没有可用的评分策略，请先发布")
        return record

    @staticmethod
    def _get_policy(db: Session, revision: int) -> QualityPolicyRecord:
        record = (
            db.query(QualityPolicyRecord)
            .filter(QualityPolicyRecord.revision == revision)
            .first()
        )
        if record is None:
            raise JobNotFoundError(f"策略版本 {revision} 不存在")
        return record

    @staticmethod
    def _get_job(db: Session, job_id: int) -> RecalculationJobRecord:
        job = (
            db.query(RecalculationJobRecord)
            .filter(RecalculationJobRecord.id == job_id)
            .first()
        )
        if job is None:
            raise JobNotFoundError(f"重算作业 {job_id} 不存在")
        return job

    def _ensure_policy_active(
        self, db: Session, job: RecalculationJobRecord
    ) -> None:
        if job.policy.is_active:
            return
        message = f"评分策略版本 {job.policy.revision} 已失效，作业停止"
        job.status = STATUS_FAILED
        job.error = message
        db.commit()
        raise PolicyStaleError(message)

    def _lease_cutoff(self) -> datetime:
        # SQLite 侧以朴素 UTC 存储，比较时去掉时区信息保持一致。
        return (self._now() - self._lease_timeout).astimezone(timezone.utc).replace(
            tzinfo=None
        )

    @staticmethod
    def _freeze(record: QualityPolicyRecord) -> FrozenPolicy:
        return FrozenPolicy(
            revision=record.revision,
            name=record.name,
            completeness_weight=record.completeness_weight,
            annotation_weight=record.annotation_weight,
            thresholds={
                "grade_a": record.grade_a_threshold,
                "grade_b": record.grade_b_threshold,
                "grade_c": record.grade_c_threshold,
            },
            fingerprint=record.fingerprint,
        )

    def _job_view(self, db: Session, job: RecalculationJobRecord) -> dict:
        finished = job.succeeded_count + job.skipped_count + job.failed_count
        return {
            "id": job.id,
            "status": job.status,
            "total_count": job.total_count,
            "claimed_count": job.claimed_count,
            "succeeded_count": job.succeeded_count,
            "skipped_count": job.skipped_count,
            "failed_count": job.failed_count,
            "finished_count": finished,
            "remaining_count": job.total_count - finished - job.claimed_count,
            "last_processed_id": job.last_processed_id,
            "progress": round(finished / job.total_count, 4) if job.total_count else 0.0,
            "error": job.error,
            "scope": job.scope,
            "created_by": job.created_by,
            "policy": self._freeze(job.policy).as_dict(),
            "created_at": job.created_at,
            "started_at": job.started_at,
            "paused_at": job.paused_at,
            "resumed_at": job.resumed_at,
            "completed_at": job.completed_at,
            "cancelled_at": job.cancelled_at,
        }

    @staticmethod
    def _batch_view(batch: RecalculationBatchRecord) -> dict:
        return {
            "id": batch.id,
            "job_id": batch.job_id,
            "sequence": batch.sequence,
            "status": batch.status,
            "claimed_by": batch.claimed_by,
            "outcome_count": batch.outcome_count,
            "success_count": batch.success_count,
            "skipped_count": batch.skipped_count,
            "failed_count": batch.failed_count,
            "created_at": batch.created_at,
            "finished_at": batch.finished_at,
        }

    @staticmethod
    def _empty_claim(job_id: int) -> Claim:
        return Claim(
            job_id=job_id,
            batch_id=-1,
            sequence=0,
            token="",
            operation_ids=[],
            claimed_by=None,
        )
