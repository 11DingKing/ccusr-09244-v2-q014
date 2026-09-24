"""重算作业编排测试。

覆盖：并发领取不重复写入、策略版本冻结与失效、取消竞态、
单项失败隔离、暂停继续、进程重启与租约恢复、在线评分接口共存。
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import (
    OperationData,
    RecalculationUnitRecord,
    RobotModel,
    Scene,
    Skill,
)
from app.database import Base
from app.services import recalculation as rc
from app.services.recalculation import (
    ClaimLostError,
    EmptyScopeError,
    JobStateError,
    PolicyStaleError,
    RecalculationOrchestrator,
    UnitOutcome,
    default_processor,
)

UTC = timezone.utc
START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class FakeClock:
    """可注入的时钟，支持跳跃推进。"""

    def __init__(self, start: datetime = START) -> None:
        self._now = start
        self._lock = threading.Lock()

    def __call__(self) -> datetime:
        with self._lock:
            return self._now

    def advance(self, delta: timedelta) -> None:
        with self._lock:
            self._now += delta


def make_engine(path: str | os.PathLike | None = None):
    if path is None:
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    else:
        engine = create_engine(
            f"sqlite:///{path}",
            connect_args={"check_same_thread": False, "timeout": 30},
        )

        @event.listens_for(engine, "connect")
        def _pragma(dbapi_conn, _record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture()
def session_factory(tmp_path):
    engine = make_engine(tmp_path / "recalc.db")
    factory = sessionmaker(bind=engine)
    yield factory
    engine.dispose()


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def orchestrator(session_factory, clock):
    instance = RecalculationOrchestrator(
        session_factory, now=clock, lease_timeout=timedelta(minutes=30)
    )
    instance.publish_policy(name="默认策略")
    return instance


def seed_operations(session_factory, count: int = 10, *, grade: str | None = None) -> list[int]:
    """创建最小可评分作业，返回按升序排列的作业ID。"""

    db = session_factory()
    model = RobotModel(name=f"M-{uuid4()}", manufacturer="厂家")
    scene = Scene(name=f"S-{uuid4()}", category="制造")
    skill = Skill(name=f"K-{uuid4()}", category="抓取")
    db.add_all([model, scene, skill])
    db.flush()
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    ids = []
    for i in range(count):
        op = OperationData(
            robot_model_id=model.id,
            scene_id=scene.id,
            skill_id=skill.id,
            motion_trajectory={"waypoints": [1, 2], "joint_angles": [0]},
            perception_records={"camera": "ok"},
            grasp_result={"ok": True},
            environment_conditions={"temp": 25},
            hardware_status={"battery": 0.9},
            duration_ms=1000 + i,
            timestamp_start=ts,
            timestamp_end=ts + timedelta(milliseconds=1000),
            data_grade=grade,
        )
        db.add(op)
        db.flush()
        ids.append(op.id)
    db.commit()
    db.close()
    return sorted(ids)


# --------------------------------------------------------------- 基础行为


def test_publish_policy_versions_and_fingerprint(session_factory, clock):
    orchestrator = RecalculationOrchestrator(
        session_factory, now=clock, lease_timeout=timedelta(minutes=30)
    )
    p1 = orchestrator.publish_policy(name="默认策略")
    p2 = orchestrator.publish_policy(name="收紧策略", grade_a_threshold=0.95)
    assert p1.revision == 1 and p2.revision == 2
    assert p1.fingerprint != p2.fingerprint

    with pytest.raises(ValueError):
        orchestrator.publish_policy(name="坏策略", completeness_weight=0.4, annotation_weight=0.4)
    with pytest.raises(ValueError):
        orchestrator.publish_policy(
            name="坏阈值",
            grade_a_threshold=0.5,
            grade_b_threshold=0.7,
            grade_c_threshold=0.9,
        )


def test_create_job_freezes_scope_and_policy(session_factory, orchestrator):
    ids = seed_operations(session_factory, 6, grade="C")
    # 范围外的作业：等级不同，不应进入作业
    other = seed_operations(session_factory, 2, grade="A")

    job = orchestrator.create_job(scope={"data_grade": "C"})
    assert job["status"] == "running"
    assert job["total_count"] == 6
    assert job["policy"]["revision"] == 1
    assert job["scope"]["filters"] == {"data_grade": "C"}

    # 作业创建后新增的作业不在冻结范围内
    extra = seed_operations(session_factory, 3, grade="C")
    results = orchestrator.run_until_done(job["id"], batch_size=4)
    touched = sorted(
        oid for result in results for oid in result.claim.operation_ids
    )
    assert touched == ids
    view = orchestrator.get_job(job["id"])
    assert view["succeeded_count"] == 6
    assert view["status"] == "completed"
    assert set(other + extra).isdisjoint(touched)


def test_empty_scope_rejected(orchestrator):
    orchestrator.publish_policy(name="默认策略")
    with pytest.raises(EmptyScopeError):
        orchestrator.create_job(scope={"robot_serial": "不存在"})


def test_claims_follow_stable_order(session_factory, orchestrator):
    ids = seed_operations(session_factory, 10)
    job = orchestrator.create_job()
    first = orchestrator.claim_batch(job["id"], batch_size=4)
    second = orchestrator.claim_batch(job["id"], batch_size=4)
    assert first.operation_ids == ids[:4]
    assert second.operation_ids == ids[4:8]


# --------------------------------------------------------------- 并发领取


def test_concurrent_claims_never_write_twice(session_factory, orchestrator):
    total = 160
    ids = set(seed_operations(session_factory, total))

    job_id = orchestrator.create_job()["id"]

    process_counts: dict[int, int] = {}
    counts_lock = threading.Lock()

    def counted_processor(db, policy, operation_id):
        with counts_lock:
            process_counts[operation_id] = process_counts.get(operation_id, 0) + 1
        return default_processor(db, policy, operation_id)

    errors: list[Exception] = []
    barrier = threading.Barrier(8)

    def worker(idx):
        try:
            barrier.wait()
            while True:
                result = orchestrator.run_batch(
                    job_id, batch_size=7, claimed_by=f"w{idx}",
                    processor=counted_processor,
                )
                if result is None:
                    return
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not errors, errors

    job = orchestrator.get_job(job_id)
    assert job["status"] == "completed"
    assert job["succeeded_count"] == total
    assert job["failed_count"] == 0
    assert job["finished_count"] == total
    assert job["remaining_count"] == 0

    # 每个作业只被有效处理一次
    assert set(process_counts) == ids
    assert all(count == 1 for count in process_counts.values())

    # 工作单元全部终态，作业分数只写了一次（值与冻结策略一致）
    db = session_factory()
    units = db.query(RecalculationUnitRecord).filter_by(job_id=job_id).all()
    assert sorted(u.status for u in units).count(rc.RESULT_SUCCESS) == total
    ops = db.query(OperationData).filter(OperationData.id.in_(ids)).all()
    assert all(op.quality_score is not None and op.data_grade is not None for op in ops)
    db.close()

    batches = orchestrator.list_batches(job_id)
    assert sum(b["outcome_count"] for b in batches) == total


def test_duplicate_report_is_rejected(session_factory, orchestrator):
    seed_operations(session_factory, 5)
    job_id = orchestrator.create_job()["id"]
    claim = orchestrator.claim_batch(job_id, batch_size=5)

    db = session_factory()
    frozen = orchestrator._load_frozen_policy(job_id)
    outcomes = [default_processor(db, frozen, oid) for oid in claim.operation_ids]
    db.close()

    first = orchestrator.report_batch(job_id, claim.batch_id, claim.token, outcomes)
    assert first["batch"]["status"] == "completed"

    # 同凭证重复回报：整批拒绝，不产生第二次写入
    with pytest.raises(ClaimLostError):
        orchestrator.report_batch(job_id, claim.batch_id, claim.token, outcomes)

    job = orchestrator.get_job(job_id)
    assert job["succeeded_count"] == 5
    assert len(orchestrator.list_outcomes(job_id)) == 5


def test_lease_expiry_reclaim_rejects_stale_token(session_factory, clock):
    """租约超时后单元可被他人接管，旧持有者的迟到回报必须拒绝。"""

    seed_operations(session_factory, 4)
    orchestrator = RecalculationOrchestrator(
        session_factory, now=clock, lease_timeout=timedelta(minutes=30)
    )
    orchestrator.publish_policy(name="默认策略")
    job_id = orchestrator.create_job()["id"]

    stale = orchestrator.claim_batch(job_id, batch_size=4, claimed_by="slow-worker")
    db = session_factory()
    frozen = orchestrator._load_frozen_policy(job_id)
    stale_outcomes = [
        default_processor(db, frozen, oid) for oid in stale.operation_ids
    ]
    db.close()

    # 超过租约
    clock.advance(timedelta(minutes=31))

    reclaimed = orchestrator.claim_batch(job_id, batch_size=4, claimed_by="takeover")
    assert reclaimed.operation_ids == stale.operation_ids
    assert reclaimed.token != stale.token

    # 旧批次标记过期，旧回报拒绝
    old_batch = [b for b in orchestrator.list_batches(job_id) if b["id"] == stale.batch_id][0]
    assert old_batch["status"] == "expired"
    with pytest.raises(ClaimLostError):
        orchestrator.report_batch(job_id, stale.batch_id, stale.token, stale_outcomes)

    # 新持有者回报成功，且只写一次
    db = session_factory()
    new_outcomes = [
        default_processor(db, frozen, oid) for oid in reclaimed.operation_ids
    ]
    db.close()
    orchestrator.report_batch(job_id, reclaimed.batch_id, reclaimed.token, new_outcomes)
    job = orchestrator.get_job(job_id)
    assert job["status"] == "completed"
    assert job["succeeded_count"] == 4
    assert job["claimed_count"] == 0


# --------------------------------------------------------------- 策略失效


def test_job_keeps_frozen_policy_when_new_version_published(session_factory, orchestrator):
    seed_operations(session_factory, 3)
    job_id = orchestrator.create_job()["id"]
    assert orchestrator.get_job(job_id)["policy"]["revision"] == 1

    orchestrator.publish_policy(name="新策略", grade_a_threshold=0.95)
    # 新作业默认使用新版本，旧作业仍冻结在版本1
    new_job_id = orchestrator.create_job()["id"]
    assert orchestrator.get_job(new_job_id)["policy"]["revision"] == 2
    assert orchestrator.get_job(job_id)["policy"]["revision"] == 1
    claim = orchestrator.claim_batch(job_id, batch_size=2)
    assert claim.operation_ids  # 旧作业照常领取


def test_policy_deactivation_fails_running_job(session_factory, orchestrator):
    seed_operations(session_factory, 6)
    job_id = orchestrator.create_job()["id"]
    first = orchestrator.claim_batch(job_id, batch_size=2)

    orchestrator.deactivate_policy(1)

    # 已完成批次仍可回报
    db = session_factory()
    frozen = orchestrator._load_frozen_policy(job_id)
    outcomes = [default_processor(db, frozen, oid) for oid in first.operation_ids]
    db.close()
    report = orchestrator.report_batch(job_id, first.batch_id, first.token, outcomes)
    assert report["batch"]["success_count"] == 2

    # 新领取被拒绝，作业标记失败并保留进度
    with pytest.raises(PolicyStaleError):
        orchestrator.claim_batch(job_id, batch_size=2)
    job = orchestrator.get_job(job_id)
    assert job["status"] == "failed"
    assert job["succeeded_count"] == 2
    assert "已失效" in job["error"]

    # 失败终态不能继续
    with pytest.raises(JobStateError):
        orchestrator.resume_job(job_id)


def test_cannot_create_job_on_inactive_policy(orchestrator):
    orchestrator.publish_policy(name="默认策略")
    orchestrator.deactivate_policy(1)
    with pytest.raises(PolicyStaleError):
        orchestrator.create_job(policy_revision=1)


# --------------------------------------------------------------- 取消竞态


def test_cancel_rejects_late_report_and_keeps_progress(session_factory, orchestrator):
    seed_operations(session_factory, 8)
    job_id = orchestrator.create_job()["id"]

    done = orchestrator.claim_batch(job_id, batch_size=3)
    db = session_factory()
    frozen = orchestrator._load_frozen_policy(job_id)
    done_outcomes = [default_processor(db, frozen, oid) for oid in done.operation_ids]
    db.close()
    orchestrator.report_batch(job_id, done.batch_id, done.token, done_outcomes)

    in_flight = orchestrator.claim_batch(job_id, batch_size=3)
    db = session_factory()
    late_outcomes = [default_processor(db, frozen, oid) for oid in in_flight.operation_ids]
    db.close()

    # 取消与回报竞态：取消先落库，迟到回报一律拒绝
    cancelled = orchestrator.cancel_job(job_id)
    assert cancelled["status"] == "cancelled"

    with pytest.raises(JobStateError):
        orchestrator.report_batch(
            job_id, in_flight.batch_id, in_flight.token, late_outcomes
        )

    # 取消后不能领取；重复取消是幂等的，继续会被拒绝
    assert orchestrator.claim_batch(job_id, batch_size=2).operation_ids == []
    assert orchestrator.cancel_job(job_id)["status"] == "cancelled"
    with pytest.raises(JobStateError):
        orchestrator.resume_job(job_id)

    # 已完成进度完整保留，在途单元未被写入
    job = orchestrator.get_job(job_id)
    assert job["succeeded_count"] == 3
    assert job["finished_count"] == 3
    db = session_factory()
    untouched = (
        db.query(OperationData)
        .filter(OperationData.id.in_(in_flight.operation_ids))
        .all()
    )
    assert all(op.quality_score is None for op in untouched)
    db.close()

    # 批次与单项结果仍可查询
    batches = orchestrator.list_batches(job_id)
    assert batches[0]["success_count"] == 3
    failures = orchestrator.list_outcomes(job_id, result="failed")
    assert failures == []


# --------------------------------------------------------------- 失败隔离


def test_single_item_failure_is_isolated(session_factory, orchestrator):
    ids = seed_operations(session_factory, 12)
    bad_ids = {ids[2], ids[7], ids[11]}
    job_id = orchestrator.create_job()["id"]

    def flaky_processor(db, policy, operation_id):
        if operation_id in bad_ids:
            raise RuntimeError(f"作业 {operation_id} 评分失败")
        return default_processor(db, policy, operation_id)

    results = orchestrator.run_until_done(
        job_id, batch_size=5, processor=flaky_processor
    )
    job = orchestrator.get_job(job_id)
    assert job["status"] == "completed"
    assert job["succeeded_count"] == 9
    assert job["failed_count"] == 3
    assert sum(r.failed_count for r in results) == 3

    failed_outcomes = orchestrator.list_outcomes(job_id, result="failed")
    assert {row["operation_id"] for row in failed_outcomes} == bad_ids
    assert all("评分失败" in row["reason"] for row in failed_outcomes)

    # 失败项没有被写入，成功项正常更新
    db = session_factory()
    ops = {op.id: op for op in db.query(OperationData).all()}
    for oid in bad_ids:
        assert ops[oid].quality_score is None
    for oid in ids:
        if oid not in bad_ids:
            assert ops[oid].quality_score is not None
    db.close()


# --------------------------------------------------------------- 暂停继续


def test_pause_resume_preserves_progress(session_factory, orchestrator):
    seed_operations(session_factory, 10)
    job_id = orchestrator.create_job()["id"]

    result = orchestrator.run_batch(job_id, batch_size=4)
    assert result.success_count == 4
    paused = orchestrator.pause_job(job_id)
    assert paused["status"] == "paused"
    assert paused["succeeded_count"] == 4
    assert paused["paused_at"] is not None

    with pytest.raises(JobStateError):
        orchestrator.claim_batch(job_id, batch_size=2)

    # 暂停是幂等的，继续后从检查点接着跑
    orchestrator.pause_job(job_id)
    resumed = orchestrator.resume_job(job_id)
    assert resumed["status"] == "running"
    orchestrator.run_until_done(job_id, batch_size=3)

    job = orchestrator.get_job(job_id)
    assert job["status"] == "completed"
    assert job["succeeded_count"] == 10
    assert job["remaining_count"] == 0


def test_pause_rejected_in_terminal_state(session_factory, orchestrator):
    seed_operations(session_factory, 2)
    job_id = orchestrator.create_job()["id"]
    orchestrator.run_until_done(job_id)
    with pytest.raises(JobStateError):
        orchestrator.pause_job(job_id)


# --------------------------------------------------------------- 重启恢复


def test_restart_orchestrator_resumes_from_checkpoint(session_factory, clock):
    op_ids = seed_operations(session_factory, 20)

    first = RecalculationOrchestrator(
        session_factory, now=clock, lease_timeout=timedelta(minutes=30)
    )
    first.publish_policy(name="默认策略")
    job_id = first.create_job()["id"]
    first.run_batch(job_id, batch_size=6)

    # “进程退出”：一个批次已领取但未回报（孤儿批次）
    orphan = first.claim_batch(job_id, batch_size=6, claimed_by="dead-worker")
    assert orphan.operation_ids == op_ids[6:12]

    # 重新启动一个全新的编排器实例：已完成的 6 条不重算，
    # 孤儿批次仍在租约内，其 6 条暂不被接管，只能领取剩余待处理单元
    second = RecalculationOrchestrator(
        session_factory, now=clock, lease_timeout=timedelta(minutes=30)
    )
    claim = second.claim_batch(job_id, batch_size=10)
    assert claim.operation_ids == op_ids[12:]
    assert second.get_job(job_id)["succeeded_count"] == 6

    # 进程再次重启、时钟越过租约：孤儿单元被接管，作业直至全部完成
    clock.advance(timedelta(minutes=31))
    third = RecalculationOrchestrator(
        session_factory, now=clock, lease_timeout=timedelta(minutes=30)
    )
    third.run_until_done(job_id, batch_size=5)

    job = third.get_job(job_id)
    assert job["status"] == "completed"
    assert job["succeeded_count"] == 20
    assert job["failed_count"] == 0

    outcomes = third.list_outcomes(job_id)
    result_op_ids = [row["operation_id"] for row in outcomes]
    assert len(result_op_ids) == len(set(result_op_ids)) == 20
    assert sorted(result_op_ids) == op_ids


# --------------------------------------------------------------- 时间注入


def test_clock_is_injected_into_timestamps(session_factory):
    clock = FakeClock()
    orchestrator = RecalculationOrchestrator(session_factory, now=clock)
    orchestrator.publish_policy(name="默认策略")
    seed_operations(session_factory, 2)
    job_id = orchestrator.create_job()["id"]
    assert orchestrator.get_job(job_id)["started_at"].replace(tzinfo=None) == START.replace(tzinfo=None)

    clock.advance(timedelta(minutes=5))
    paused = orchestrator.pause_job(job_id)
    assert paused["paused_at"].replace(tzinfo=None) == (
        START + timedelta(minutes=5)
    ).replace(tzinfo=None)

    clock.advance(timedelta(minutes=10))
    resumed = orchestrator.resume_job(job_id)
    assert resumed["resumed_at"].replace(tzinfo=None) == (
        START + timedelta(minutes=15)
    ).replace(tzinfo=None)


# --------------------------------------------------------------- 在线接口共存


def _build_test_client(session_factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.database import get_db
    from app.routers import analytics, recalculation as recalc_router
    import app.routers.recalculation as recalc_module

    recalc_module.SessionLocal = session_factory

    app = FastAPI()
    app.include_router(analytics.router, prefix="/api/v1")
    app.include_router(recalc_router.router, prefix="/api/v1")

    def _override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _override_get_db
    return TestClient(app)


def test_online_grading_endpoint_still_works(session_factory, orchestrator):
    ids = seed_operations(session_factory, 5)

    client = _build_test_client(session_factory)

    # 原有在线评分接口行为不变
    resp = client.post(
        "/api/v1/quality/grade-operations",
        json={
            "completeness_weight": 0.5,
            "annotation_weight": 0.5,
            "grade_a_threshold": 0.9,
            "grade_b_threshold": 0.7,
            "grade_c_threshold": 0.5,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["message"] == "已完成 5 条数据的质量分级"

    # 重算接口共存：端到端创建、跑批、查批次与失败原因
    created = client.post("/api/v1/quality/recalculation-jobs", json={}).json()
    job_id = created["id"]
    run = client.post(f"/api/v1/quality/recalculation-jobs/{job_id}/run-batch?batch_size=3")
    assert run.status_code == 200
    body = run.json()
    assert body["success_count"] == 3
    assert body["job"]["total_count"] == 5

    pause = client.post(f"/api/v1/quality/recalculation-jobs/{job_id}/pause")
    assert pause.status_code == 200
    claim_while_paused = client.post(
        f"/api/v1/quality/recalculation-jobs/{job_id}/claim",
        json={"batch_size": 2},
    )
    assert claim_while_paused.status_code == 409
    client.post(f"/api/v1/quality/recalculation-jobs/{job_id}/resume")

    while True:
        body = client.post(
            f"/api/v1/quality/recalculation-jobs/{job_id}/run-batch?batch_size=3"
        ).json()
        if body.get("empty"):
            break

    detail = client.get(f"/api/v1/quality/recalculation-jobs/{job_id}").json()
    assert detail["status"] == "completed"
    assert detail["succeeded_count"] == 5

    batches = client.get(f"/api/v1/quality/recalculation-jobs/{job_id}/batches").json()
    assert sum(b["success_count"] for b in batches) == 5
    outcomes = client.get(f"/api/v1/quality/recalculation-jobs/{job_id}/outcomes").json()
    assert len(outcomes) == 5


def test_api_cancel_and_invalid_policy(session_factory, orchestrator):
    seed_operations(session_factory, 4)
    client = _build_test_client(session_factory)

    job_id = client.post("/api/v1/quality/recalculation-jobs", json={}).json()["id"]
    claimed = client.post(
        f"/api/v1/quality/recalculation-jobs/{job_id}/claim",
        json={"batch_size": 4},
    ).json()
    assert len(claimed["operation_ids"]) == 4

    assert client.post(f"/api/v1/quality/recalculation-jobs/{job_id}/cancel").status_code == 200

    # 迟到回报被拒绝
    outcomes = [
        {"operation_id": oid, "result": "success",
         "completeness_score": 1.0, "quality_score": 1.0, "after_grade": "A"}
        for oid in claimed["operation_ids"]
    ]
    resp = client.post(
        f"/api/v1/quality/recalculation-jobs/{job_id}/batches/{claimed['batch_id']}/report",
        json={"token": claimed["token"], "outcomes": outcomes},
    )
    assert resp.status_code == 409

    # 策略列表与作废
    policies = client.get("/api/v1/quality/policies").json()
    assert policies[0]["revision"] == 1
    deactivated = client.post("/api/v1/quality/policies/1/deactivate").json()
    assert deactivated["is_active"] is False
