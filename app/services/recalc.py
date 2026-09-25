"""历史作业质量重算的进程内编排服务。

评分策略调整后，质量负责人在业务低峰创建重算作业，由多个工作协程分批执行：

- 创建作业时冻结策略参数与筛选范围，后续策略或数据变化不影响本作业；
- 工作单元按稳定顺序（作业数据 ID 升序切批）领取，条件更新保证同一单元
  不会被两个领取者同时获得；
- 完成与失败提交都校验收取令牌，重复领取或重复提交不会二次写入；
- 暂停、继续和取消都保留已完成进度，取消后不再接受新的结果写入；
- 进程退出后再次启动时调用 :meth:`RecalcManager.recover`，把崩溃时处于
  领取状态的单元重置为待领取，从持久化检查点继续；
- 时间判断通过 ``clock`` 注入，评分逻辑通过 ``scorer`` 注入，便于稳定验证。

管理器在进程内用一把可重入锁串行化所有状态变更；跨进程/重启场景由
数据库层面的条件更新兜底。
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from app.models import Annotation, OperationData, RecalcItem, RecalcJob, RecalcUnit
from app.services.scoring import QualityScores, compute_operation_quality

# 作业状态
JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_PAUSED = "paused"
JOB_COMPLETED = "completed"
JOB_CANCELLED = "cancelled"
JOB_FAILED = "failed"
JOB_ACTIVE = (JOB_PENDING, JOB_RUNNING, JOB_PAUSED)
JOB_TERMINAL = (JOB_COMPLETED, JOB_CANCELLED, JOB_FAILED)

# 工作单元状态
UNIT_PENDING = "pending"
UNIT_CLAIMED = "claimed"
UNIT_COMPLETED = "completed"
UNIT_FAILED = "failed"
UNIT_SKIPPED = "skipped"

# 单项结果状态
ITEM_PENDING = "pending"
ITEM_SUCCESS = "success"
ITEM_SKIPPED = "skipped"
ITEM_FAILED = "failed"

REASON_JOB_CANCELLED = "作业已取消"
REASON_POLICY_EXPIRED = "策略已失效"
REASON_OPERATION_MISSING = "作业数据不存在"

MAX_ITEM_REASON_ROWS = 100
ALLOWED_FILTERS = ("robot_model_id", "scene_id", "skill_id", "data_grade", "is_annotated")


class RecalcError(Exception):
    """重算编排的基础错误。"""


class RecalcValidationError(RecalcError):
    """作业参数不合法。"""


class JobNotFoundError(RecalcError):
    """重算作业不存在。"""


class InvalidJobStateError(RecalcError):
    """当前作业状态不允许该操作。"""


class ClaimLostError(RecalcError):
    """领取令牌失效：单元已被回收、重复领取或租约过期。"""


class JobCancelledError(ClaimLostError):
    """作业已取消，结果不再接收。"""


class PolicyExpiredError(RecalcError):
    """冻结策略已超过失效时间，剩余单元被跳过。"""


@dataclass(frozen=True)
class FrozenPolicy:
    """创建作业时冻结的评分策略。``expires_at`` 为朴素 UTC 时间。"""

    completeness_weight: float
    annotation_weight: float
    thresholds: dict[str, float]
    expires_at: Optional[datetime] = None
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "completeness_weight": self.completeness_weight,
            "annotation_weight": self.annotation_weight,
            "thresholds": dict(self.thresholds),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "note": self.note,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "FrozenPolicy":
        raw_expires = data.get("expires_at")
        expires_at = datetime.fromisoformat(raw_expires) if raw_expires else None
        return FrozenPolicy(
            completeness_weight=float(data["completeness_weight"]),
            annotation_weight=float(data["annotation_weight"]),
            thresholds={key: float(value) for key, value in data["thresholds"].items()},
            expires_at=expires_at,
            note=data.get("note") or "",
        )


def build_frozen_policy(
    *,
    completeness_weight: float,
    annotation_weight: float,
    grade_a_threshold: float,
    grade_b_threshold: float,
    grade_c_threshold: float,
    expires_at: Optional[datetime] = None,
    note: str = "",
) -> FrozenPolicy:
    """校验并构造冻结策略；``expires_at`` 必须带时区，内部统一为朴素 UTC。"""
    for label, value in (("完整度权重", completeness_weight), ("标注质量权重", annotation_weight)):
        if not 0.0 <= value <= 1.0:
            raise RecalcValidationError(f"{label}必须位于零到一之间")
    if abs(completeness_weight + annotation_weight - 1.0) > 1e-6:
        raise RecalcValidationError("完整度权重和标注质量权重之和必须为1.0")
    thresholds = {
        "grade_a": float(grade_a_threshold),
        "grade_b": float(grade_b_threshold),
        "grade_c": float(grade_c_threshold),
    }
    if not 0.0 <= thresholds["grade_c"] <= thresholds["grade_b"] <= thresholds["grade_a"] <= 1.0:
        raise RecalcValidationError("分级阈值必须满足 0 <= C <= B <= A <= 1")
    normalized_expires: Optional[datetime] = None
    if expires_at is not None:
        if expires_at.tzinfo is None:
            raise RecalcValidationError("策略失效时间必须带时区")
        normalized_expires = expires_at.astimezone(timezone.utc).replace(tzinfo=None)
    return FrozenPolicy(
        completeness_weight=float(completeness_weight),
        annotation_weight=float(annotation_weight),
        thresholds=thresholds,
        expires_at=normalized_expires,
        note=note.strip(),
    )


ClockFn = Callable[[], datetime]
ScorerFn = Callable[[OperationData, Optional[Annotation], FrozenPolicy], QualityScores]


def default_clock() -> datetime:
    return datetime.now(timezone.utc)


def default_scorer(
    operation: OperationData,
    annotation: Optional[Annotation],
    policy: FrozenPolicy,
) -> QualityScores:
    return compute_operation_quality(
        operation=operation,
        annotation=annotation,
        completeness_weight=policy.completeness_weight,
        annotation_weight=policy.annotation_weight,
        thresholds=policy.thresholds,
    )


class RecalcManager:
    """进程内重算编排器：所有状态变更在管理器锁内完成。"""

    def __init__(
        self,
        clock: ClockFn = default_clock,
        scorer: ScorerFn = default_scorer,
        lease_seconds: int = 300,
    ) -> None:
        if lease_seconds < 1:
            raise RecalcValidationError("租约时长必须为正")
        self._clock = clock
        self._scorer = scorer
        self._lease_seconds = lease_seconds
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> datetime:
        moment = self._clock()
        if moment.tzinfo is None:
            raise RecalcValidationError("时钟必须返回带时区的时间")
        return moment.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _get_job(db: Session, job_id: int) -> RecalcJob:
        job = db.get(RecalcJob, job_id)
        if job is None:
            raise JobNotFoundError(f"重算作业 {job_id} 不存在")
        return job

    @staticmethod
    def _get_unit(db: Session, job_id: int, unit_id: int) -> RecalcUnit:
        unit = db.get(RecalcUnit, unit_id)
        if unit is None or unit.job_id != job_id:
            raise RecalcValidationError(f"工作单元 {unit_id} 不属于作业 {job_id}")
        return unit

    @staticmethod
    def _fresh(db: Session, *instances) -> None:
        """在管理器锁内重读行状态，避免长生命周期会话的缓存掩盖其他会话已提交的变更。"""
        for instance in instances:
            db.refresh(instance)

    @staticmethod
    def _policy_expired(job: RecalcJob, now: datetime) -> bool:
        return job.policy_expires_at is not None and now >= job.policy_expires_at

    @staticmethod
    def _validate_filters(filters: Optional[dict[str, Any]]) -> dict[str, Any]:
        filters = dict(filters or {})
        unknown = sorted(set(filters) - set(ALLOWED_FILTERS))
        if unknown:
            raise RecalcValidationError(f"不支持的筛选条件: {', '.join(unknown)}")
        normalized: dict[str, Any] = {}
        for key in ("robot_model_id", "scene_id", "skill_id"):
            if filters.get(key) is not None:
                normalized[key] = int(filters[key])
        if filters.get("data_grade") is not None:
            normalized["data_grade"] = str(filters["data_grade"])
        if filters.get("is_annotated") is not None:
            normalized["is_annotated"] = bool(filters["is_annotated"])
        return normalized

    # ------------------------------------------------------------------
    # 作业创建
    # ------------------------------------------------------------------

    def create_job(
        self,
        db: Session,
        *,
        name: str,
        policy: FrozenPolicy,
        filters: Optional[dict[str, Any]] = None,
        batch_size: int = 50,
        lease_seconds: Optional[int] = None,
        created_by: Optional[str] = None,
    ) -> RecalcJob:
        """冻结策略与筛选范围，按作业数据 ID 升序切分工作单元。"""
        if not name or not name.strip():
            raise RecalcValidationError("作业名称不能为空")
        if not 1 <= batch_size <= 500:
            raise RecalcValidationError("批大小必须位于 1 到 500 之间")
        lease = lease_seconds if lease_seconds is not None else self._lease_seconds
        if lease < 1:
            raise RecalcValidationError("租约时长必须为正")
        normalized_filters = self._validate_filters(filters)

        with self._lock:
            now = self._now()
            query = db.query(OperationData.id)
            if normalized_filters.get("robot_model_id") is not None:
                query = query.filter(OperationData.robot_model_id == normalized_filters["robot_model_id"])
            if normalized_filters.get("scene_id") is not None:
                query = query.filter(OperationData.scene_id == normalized_filters["scene_id"])
            if normalized_filters.get("skill_id") is not None:
                query = query.filter(OperationData.skill_id == normalized_filters["skill_id"])
            if normalized_filters.get("data_grade") is not None:
                query = query.filter(OperationData.data_grade == normalized_filters["data_grade"])
            if normalized_filters.get("is_annotated") is not None:
                query = query.outerjoin(Annotation, OperationData.id == Annotation.operation_data_id)
                if normalized_filters["is_annotated"]:
                    query = query.filter(Annotation.id.isnot(None))
                else:
                    query = query.filter(Annotation.id.is_(None))
            operation_ids = [row[0] for row in query.order_by(OperationData.id.asc()).all()]

            job = RecalcJob(
                name=name.strip(),
                status=JOB_PENDING,
                policy_snapshot=policy.as_dict(),
                filters=normalized_filters,
                batch_size=batch_size,
                lease_seconds=lease,
                policy_expires_at=policy.expires_at,
                created_by=created_by,
                total_items=len(operation_ids),
            )
            db.add(job)
            db.flush()

            unit_count = 0
            for start in range(0, len(operation_ids), batch_size):
                unit = RecalcUnit(job_id=job.id, unit_index=unit_count, status=UNIT_PENDING)
                db.add(unit)
                db.flush()
                for operation_id in operation_ids[start:start + batch_size]:
                    db.add(RecalcItem(job_id=job.id, unit_id=unit.id, operation_id=operation_id))
                unit_count += 1

            job.total_units = unit_count
            if unit_count == 0:
                job.status = JOB_COMPLETED
                job.finished_at = now
            db.commit()
            db.refresh(job)
            return job

    # ------------------------------------------------------------------
    # 领取与提交
    # ------------------------------------------------------------------

    def claim_next_unit(
        self,
        db: Session,
        *,
        job_id: int,
        worker_id: str,
    ) -> Optional[dict[str, Any]]:
        """按稳定顺序领取下一个工作单元；无可用单元时返回 None。

        同一单元只会被一个领取者获得：候选行通过条件更新抢占，
        租约过期的领取会被回收重新发放。
        """
        if not worker_id or not worker_id.strip():
            raise RecalcValidationError("领取者标识不能为空")
        worker_id = worker_id.strip()

        with self._lock:
            job = self._get_job(db, job_id)
            self._fresh(db, job)
            if job.status in JOB_TERMINAL or job.status == JOB_PAUSED:
                return None
            now = self._now()
            if self._policy_expired(job, now):
                self._expire_job(db, job, now)
                db.commit()
                return None
            if job.status == JOB_PENDING:
                job.status = JOB_RUNNING
                job.started_at = now

            candidates = (
                db.query(RecalcUnit)
                .filter(RecalcUnit.job_id == job.id, RecalcUnit.status.in_([UNIT_PENDING, UNIT_CLAIMED]))
                .order_by(RecalcUnit.unit_index.asc())
                .all()
            )
            for unit in candidates:
                db.refresh(unit)
                lease_expired = (
                    unit.status == UNIT_CLAIMED
                    and unit.lease_expires_at is not None
                    and unit.lease_expires_at <= now
                )
                if unit.status != UNIT_PENDING and not lease_expired:
                    continue
                token = uuid.uuid4().hex
                lease_expires_at = now + timedelta(seconds=job.lease_seconds)
                updated = (
                    db.query(RecalcUnit)
                    .filter(RecalcUnit.id == unit.id)
                    .filter(
                        or_(
                            RecalcUnit.status == UNIT_PENDING,
                            and_(
                                RecalcUnit.status == UNIT_CLAIMED,
                                RecalcUnit.lease_expires_at <= now,
                            ),
                        )
                    )
                    .update(
                        {
                            "status": UNIT_CLAIMED,
                            "claim_token": token,
                            "worker_id": worker_id,
                            "claimed_at": now,
                            "lease_expires_at": lease_expires_at,
                            "attempts": RecalcUnit.attempts + 1,
                        },
                        synchronize_session=False,
                    )
                )
                if not updated:
                    continue  # 被其他领取者抢先，尝试下一个候选
                db.refresh(unit)
                operation_ids = [
                    row[0]
                    for row in db.query(RecalcItem.operation_id)
                    .filter(RecalcItem.unit_id == unit.id)
                    .order_by(RecalcItem.operation_id.asc())
                    .all()
                ]
                db.commit()
                return {
                    "job_id": job.id,
                    "unit_id": unit.id,
                    "unit_index": unit.unit_index,
                    "claim_token": token,
                    "worker_id": worker_id,
                    "operation_ids": operation_ids,
                    "claimed_at": now.isoformat(),
                    "lease_expires_at": lease_expires_at.isoformat(),
                    "attempts": unit.attempts,
                }
            db.commit()
            return None

    def complete_unit(
        self,
        db: Session,
        *,
        job_id: int,
        unit_id: int,
        claim_token: str,
    ) -> dict[str, Any]:
        """处理并提交一个已领取的单元。

        单项失败只记录原因并继续（失败隔离）；已成功过的单项不会二次写入；
        重复提交已完成单元是幂等空操作。
        """
        with self._lock:
            job = self._get_job(db, job_id)
            unit = self._get_unit(db, job_id, unit_id)
            self._fresh(db, job, unit)
            if unit.status == UNIT_COMPLETED:
                return self._unit_outcome(db, unit)
            self._ensure_job_accepting(job)
            now = self._now()
            self._ensure_policy_valid(db, job, now)
            self._ensure_claim_holder(unit, claim_token, now)

            policy = FrozenPolicy.from_dict(job.policy_snapshot)
            success = skipped = failed = 0
            items = (
                db.query(RecalcItem)
                .filter(RecalcItem.unit_id == unit.id)
                .order_by(RecalcItem.operation_id.asc())
                .all()
            )
            for item in items:
                if item.status == ITEM_SUCCESS:
                    success += 1  # 已写入过，跳过，保证不二次写入
                    continue
                if item.status != ITEM_PENDING:
                    if item.status == ITEM_SKIPPED:
                        skipped += 1
                    else:
                        failed += 1
                    continue
                operation = db.get(OperationData, item.operation_id)
                if operation is None:
                    item.status = ITEM_SKIPPED
                    item.reason = REASON_OPERATION_MISSING
                    item.finished_at = now
                    skipped += 1
                    continue
                annotation = (
                    db.query(Annotation)
                    .filter(Annotation.operation_data_id == item.operation_id)
                    .first()
                )
                try:
                    scores = self._scorer(operation, annotation, policy)
                except Exception as exc:  # 单项失败隔离，不影响同批其他记录
                    item.status = ITEM_FAILED
                    item.reason = f"{type(exc).__name__}: {exc}"[:500]
                    item.finished_at = now
                    failed += 1
                    continue
                operation.completeness_score = scores.completeness_score
                operation.quality_score = scores.quality_score
                operation.data_grade = scores.data_grade
                item.status = ITEM_SUCCESS
                item.reason = None
                item.finished_at = now
                success += 1

            unit.status = UNIT_COMPLETED
            unit.finished_at = now
            unit.success_count = success
            unit.skipped_count = skipped
            unit.failed_count = failed
            job.completed_units += 1
            job.success_items += success
            job.skipped_items += skipped
            job.failed_items += failed
            self._maybe_finish_job(job, now)
            db.commit()
            return self._unit_outcome(db, unit)

    def fail_unit(
        self,
        db: Session,
        *,
        job_id: int,
        unit_id: int,
        claim_token: str,
        reason: str,
    ) -> dict[str, Any]:
        """领取者主动上报单元级失败；未完成的单项记为失败并保留原因。"""
        if not reason or not reason.strip():
            raise RecalcValidationError("失败原因不能为空")
        reason = reason.strip()[:500]

        with self._lock:
            job = self._get_job(db, job_id)
            unit = self._get_unit(db, job_id, unit_id)
            self._fresh(db, job, unit)
            if unit.status == UNIT_FAILED:
                return self._unit_outcome(db, unit)
            if unit.status == UNIT_COMPLETED:
                raise InvalidJobStateError("工作单元已完成，不能再标记失败")
            self._ensure_job_accepting(job)
            now = self._now()
            self._ensure_policy_valid(db, job, now)
            self._ensure_claim_holder(unit, claim_token, now)

            failed_items = 0
            for item in db.query(RecalcItem).filter(RecalcItem.unit_id == unit.id, RecalcItem.status == ITEM_PENDING).all():
                item.status = ITEM_FAILED
                item.reason = reason
                item.finished_at = now
                failed_items += 1

            unit.status = UNIT_FAILED
            unit.reason = reason
            unit.finished_at = now
            unit.failed_count += failed_items
            job.failed_units += 1
            job.failed_items += failed_items
            self._maybe_finish_job(job, now)
            db.commit()
            return self._unit_outcome(db, unit)

    # ------------------------------------------------------------------
    # 暂停 / 继续 / 取消 / 恢复
    # ------------------------------------------------------------------

    def pause_job(self, db: Session, *, job_id: int) -> RecalcJob:
        """暂停发放新单元；已领取的单元仍可提交，已完成进度保留。"""
        with self._lock:
            job = self._get_job(db, job_id)
            self._fresh(db, job)
            if job.status == JOB_PAUSED:
                return job
            if job.status not in (JOB_PENDING, JOB_RUNNING):
                raise InvalidJobStateError("仅待执行或执行中的作业可以暂停")
            job.status = JOB_PAUSED
            db.commit()
            db.refresh(job)
            return job

    def resume_job(self, db: Session, *, job_id: int) -> RecalcJob:
        with self._lock:
            job = self._get_job(db, job_id)
            self._fresh(db, job)
            if job.status != JOB_PAUSED:
                raise InvalidJobStateError("仅暂停状态的作业可以继续")
            job.status = JOB_RUNNING
            db.commit()
            db.refresh(job)
            return job

    def cancel_job(self, db: Session, *, job_id: int) -> RecalcJob:
        """取消作业：已完成进度保留，未完成的单元与单项标记为跳过。"""
        with self._lock:
            job = self._get_job(db, job_id)
            self._fresh(db, job)
            if job.status == JOB_CANCELLED:
                return job
            if job.status in (JOB_COMPLETED, JOB_FAILED):
                raise InvalidJobStateError("作业已终结，无法取消")
            now = self._now()
            self._skip_remaining(db, job, now, REASON_JOB_CANCELLED)
            job.status = JOB_CANCELLED
            job.finished_at = now
            db.commit()
            db.refresh(job)
            return job

    def recover(self, db: Session) -> dict[str, Any]:
        """进程重启后恢复：把进行中作业的崩溃领取重置为待领取。"""
        with self._lock:
            units = (
                db.query(RecalcUnit)
                .join(RecalcJob, RecalcUnit.job_id == RecalcJob.id)
                .filter(RecalcJob.status.in_(JOB_ACTIVE), RecalcUnit.status == UNIT_CLAIMED)
                .all()
            )
            job_ids = sorted({unit.job_id for unit in units})
            for unit in units:
                unit.status = UNIT_PENDING
                unit.claim_token = None
                unit.worker_id = None
                unit.claimed_at = None
                unit.lease_expires_at = None
            db.commit()
            return {"recovered_units": len(units), "job_ids": job_ids}

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_job(self, db: Session, job_id: int) -> RecalcJob:
        return self._get_job(db, job_id)

    def job_progress(self, db: Session, job_id: int) -> dict[str, Any]:
        job = self._get_job(db, job_id)
        db.refresh(job)  # 长生命周期会话也可能读到最新进度
        status_counts = dict(
            db.query(RecalcUnit.status, func.count())
            .filter(RecalcUnit.job_id == job.id)
            .group_by(RecalcUnit.status)
            .all()
        )
        done_units = job.completed_units + job.failed_units + job.skipped_units
        progress = 1.0 if job.total_units == 0 else round(done_units / job.total_units, 4)
        return {
            "job_id": job.id,
            "name": job.name,
            "status": job.status,
            "created_by": job.created_by,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            "policy": dict(job.policy_snapshot),
            "filters": dict(job.filters),
            "batch_size": job.batch_size,
            "lease_seconds": job.lease_seconds,
            "total_units": job.total_units,
            "completed_units": job.completed_units,
            "failed_units": job.failed_units,
            "skipped_units": job.skipped_units,
            "claimed_units": status_counts.get(UNIT_CLAIMED, 0),
            "pending_units": status_counts.get(UNIT_PENDING, 0),
            "total_items": job.total_items,
            "success_items": job.success_items,
            "failed_items": job.failed_items,
            "skipped_items": job.skipped_items,
            "progress": progress,
            "error": job.error,
        }

    def list_jobs(self, db: Session, status: Optional[str] = None) -> list[dict[str, Any]]:
        query = db.query(RecalcJob)
        if status:
            query = query.filter(RecalcJob.status == status)
        return [self.job_progress(db, job.id) for job in query.order_by(RecalcJob.id.desc()).all()]

    def unit_results(self, db: Session, job_id: int) -> list[dict[str, Any]]:
        """按批返回成功、跳过、失败数量及单项原因，供质量负责人核对。"""
        self._get_job(db, job_id)
        units = (
            db.query(RecalcUnit)
            .filter(RecalcUnit.job_id == job_id)
            .order_by(RecalcUnit.unit_index.asc())
            .all()
        )
        results = []
        for unit in units:
            problem_items = (
                db.query(RecalcItem)
                .filter(RecalcItem.unit_id == unit.id, RecalcItem.status.in_([ITEM_FAILED, ITEM_SKIPPED]))
                .order_by(RecalcItem.operation_id.asc())
                .limit(MAX_ITEM_REASON_ROWS + 1)
                .all()
            )
            truncated = len(problem_items) > MAX_ITEM_REASON_ROWS
            results.append(
                {
                    "unit_id": unit.id,
                    "unit_index": unit.unit_index,
                    "status": unit.status,
                    "worker_id": unit.worker_id,
                    "attempts": unit.attempts,
                    "reason": unit.reason,
                    "success_count": unit.success_count,
                    "skipped_count": unit.skipped_count,
                    "failed_count": unit.failed_count,
                    "claimed_at": unit.claimed_at.isoformat() if unit.claimed_at else None,
                    "finished_at": unit.finished_at.isoformat() if unit.finished_at else None,
                    "item_results": [
                        {"operation_id": item.operation_id, "status": item.status, "reason": item.reason}
                        for item in problem_items[:MAX_ITEM_REASON_ROWS]
                    ],
                    "item_results_truncated": truncated,
                }
            )
        return results

    # ------------------------------------------------------------------
    # 内部状态迁移
    # ------------------------------------------------------------------

    def _ensure_job_accepting(self, job: RecalcJob) -> None:
        if job.status == JOB_CANCELLED:
            raise JobCancelledError("作业已取消，结果不再接收")
        if job.status in (JOB_COMPLETED, JOB_FAILED):
            raise InvalidJobStateError(f"作业已终结（{job.status}），结果不再接收")

    def _ensure_policy_valid(self, db: Session, job: RecalcJob, now: datetime) -> None:
        if self._policy_expired(job, now):
            self._expire_job(db, job, now)
            db.commit()
            raise PolicyExpiredError("策略已失效，剩余工作单元已跳过")

    @staticmethod
    def _ensure_claim_holder(unit: RecalcUnit, claim_token: str, now: datetime) -> None:
        if unit.status != UNIT_CLAIMED or unit.claim_token != claim_token:
            raise ClaimLostError("工作单元已被回收或重复领取，本次提交被拒绝")
        if unit.lease_expires_at is not None and now > unit.lease_expires_at:
            raise ClaimLostError("领取租约已过期，本次提交被拒绝")

    def _skip_remaining(self, db: Session, job: RecalcJob, now: datetime, reason: str) -> None:
        units = (
            db.query(RecalcUnit)
            .filter(RecalcUnit.job_id == job.id, RecalcUnit.status.in_([UNIT_PENDING, UNIT_CLAIMED]))
            .all()
        )
        for unit in units:
            unit.status = UNIT_SKIPPED
            unit.reason = reason
            unit.finished_at = now
            pending_items = (
                db.query(RecalcItem)
                .filter(RecalcItem.unit_id == unit.id, RecalcItem.status == ITEM_PENDING)
                .all()
            )
            for item in pending_items:
                item.status = ITEM_SKIPPED
                item.reason = reason
                item.finished_at = now
            unit.skipped_count += len(pending_items)
            job.skipped_units += 1
            job.skipped_items += len(pending_items)

    def _expire_job(self, db: Session, job: RecalcJob, now: datetime) -> None:
        self._skip_remaining(db, job, now, REASON_POLICY_EXPIRED)
        job.status = JOB_FAILED
        job.error = "策略已失效，剩余工作单元已跳过"
        job.finished_at = now

    @staticmethod
    def _maybe_finish_job(job: RecalcJob, now: datetime) -> None:
        done_units = job.completed_units + job.failed_units + job.skipped_units
        if job.status in JOB_ACTIVE and done_units >= job.total_units:
            job.status = JOB_COMPLETED
            job.finished_at = now

    @staticmethod
    def _unit_outcome(db: Session, unit: RecalcUnit) -> dict[str, Any]:
        failures = (
            db.query(RecalcItem)
            .filter(RecalcItem.unit_id == unit.id, RecalcItem.status.in_([ITEM_FAILED, ITEM_SKIPPED]))
            .order_by(RecalcItem.operation_id.asc())
            .all()
        )
        return {
            "job_id": unit.job_id,
            "unit_id": unit.id,
            "unit_index": unit.unit_index,
            "status": unit.status,
            "success_count": unit.success_count,
            "skipped_count": unit.skipped_count,
            "failed_count": unit.failed_count,
            "item_results": [
                {"operation_id": item.operation_id, "status": item.status, "reason": item.reason}
                for item in failures
            ],
        }


# 应用级单例：路由与启动恢复共用；测试可自行构造注入时钟与评分器的实例。
recalc_manager = RecalcManager()
