import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from app.models import Annotation, OperationData, RecalcItem, RecalcUnit, RobotModel, Scene, Skill
from app.services.recalc import (
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    REASON_JOB_CANCELLED,
    REASON_POLICY_EXPIRED,
    UNIT_COMPLETED,
    UNIT_SKIPPED,
    ClaimLostError,
    InvalidJobStateError,
    JobCancelledError,
    PolicyExpiredError,
    RecalcManager,
    build_frozen_policy,
    default_scorer,
)

UTC = timezone.utc


class FakeClock:
    """可推进的注入时钟，保证时间判断可稳定验证。"""

    def __init__(self, start=None):
        self.moment = start or datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self):
        return self.moment

    def advance(self, **kwargs):
        self.moment += timedelta(**kwargs)


def make_policy(**overrides):
    params = dict(
        completeness_weight=0.5,
        annotation_weight=0.5,
        grade_a_threshold=0.9,
        grade_b_threshold=0.7,
        grade_c_threshold=0.5,
    )
    params.update(overrides)
    return build_frozen_policy(**params)


def ensure_base(session):
    robot_model = RobotModel(name="机械臂A", manufacturer="测试厂商")
    scene = Scene(name="装配线", category="生产制造")
    skill = Skill(name="抓取", category="操作")
    session.add_all([robot_model, scene, skill])
    session.flush()
    return robot_model.id, scene.id, skill.id


def add_operations(session, robot_model_id, scene_id, skill_id, count, partial=False):
    ids = []
    for _ in range(count):
        if partial:
            trajectory = {"waypoints": [{"x": 1}]}
            perception = {}
            grasp = None
        else:
            trajectory = {"waypoints": [{"x": 1}], "joint_angles": [[0.1]]}
            perception = {"rgb": 1, "depth": 2, "lidar": 3, "imu": 4, "force": 5}
            grasp = {"success": True}
        operation = OperationData(
            robot_model_id=robot_model_id,
            scene_id=scene_id,
            skill_id=skill_id,
            motion_trajectory=trajectory,
            perception_records=perception,
            grasp_result=grasp,
            environment_conditions=None if partial else {"temperature": 25},
            hardware_status=None if partial else {"servo": "ok"},
            duration_ms=None if partial else 120,
            timestamp_start=datetime(2025, 6, 1, 8, 0, 0),
            timestamp_end=datetime(2025, 6, 1, 8, 1, 0),
        )
        session.add(operation)
        session.flush()
        ids.append(operation.id)
    session.commit()
    return ids


def seed(session, count, **kwargs):
    robot_model_id, scene_id, skill_id = ensure_base(session)
    ids = add_operations(session, robot_model_id, scene_id, skill_id, count, **kwargs)
    return ids, robot_model_id


def drain_job(manager, session_factory, job_id, worker_id="drain"):
    session = session_factory()
    completed = 0
    try:
        while True:
            claim = manager.claim_next_unit(session, job_id=job_id, worker_id=worker_id)
            if claim is None:
                return completed
            manager.complete_unit(session, job_id=job_id, unit_id=claim["unit_id"], claim_token=claim["claim_token"])
            completed += 1
    finally:
        session.close()


def scored_operation_ids(session):
    return {
        row[0]
        for row in session.query(OperationData.id).filter(OperationData.quality_score.isnot(None)).all()
    }


def test_concurrent_claims_have_no_duplicates(session_factory):
    """并发领取：同一单元不会被两个协程拿到，全部单元恰好完成一次。"""
    session = session_factory()
    seed(session, 30)
    session.close()

    manager = RecalcManager(clock=FakeClock())
    session = session_factory()
    job = manager.create_job(session, name="并发领取", policy=make_policy(), batch_size=3)
    job_id = job.id
    assert job.total_units == 10
    session.close()

    claimed = []
    errors = []
    lock = threading.Lock()

    def worker(index):
        session = session_factory()
        try:
            while True:
                claim = manager.claim_next_unit(session, job_id=job_id, worker_id=f"worker-{index}")
                if claim is None:
                    return
                with lock:
                    claimed.append(claim)
                manager.complete_unit(
                    session, job_id=job_id, unit_id=claim["unit_id"], claim_token=claim["claim_token"]
                )
        except Exception as exc:  # pragma: no cover - 失败时展示具体异常
            with lock:
                errors.append(exc)
        finally:
            session.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(claimed) == 10
    assert len({claim["unit_id"] for claim in claimed}) == 10
    assert len({claim["claim_token"] for claim in claimed}) == 10

    session = session_factory()
    progress = manager.job_progress(session, job_id)
    assert progress["status"] == JOB_COMPLETED
    assert progress["completed_units"] == 10
    assert progress["success_items"] == 30
    assert progress["failed_items"] == 0
    units = manager.unit_results(session, job_id)
    assert all(unit["status"] == UNIT_COMPLETED and unit["attempts"] == 1 for unit in units)
    assert len(scored_operation_ids(session)) == 30
    session.close()


def test_duplicate_completion_is_idempotent(session_factory):
    """重复提交已完成单元是幂等空操作，不会二次写入也不会重复计数。"""
    session = session_factory()
    op_ids, _ = seed(session, 4)
    session.close()

    manager = RecalcManager(clock=FakeClock())
    session = session_factory()
    job = manager.create_job(session, name="幂等提交", policy=make_policy(), batch_size=4)
    job_id = job.id
    claim = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")
    first = manager.complete_unit(session, job_id=job_id, unit_id=claim["unit_id"], claim_token=claim["claim_token"])
    assert first["success_count"] == 4
    scores_snapshot = {
        row.id: row.quality_score for row in session.query(OperationData).filter(OperationData.id.in_(op_ids)).all()
    }

    second = manager.complete_unit(session, job_id=job_id, unit_id=claim["unit_id"], claim_token=claim["claim_token"])
    assert second["status"] == UNIT_COMPLETED
    progress = manager.job_progress(session, job_id)
    assert progress["completed_units"] == 1
    assert progress["success_items"] == 4
    for operation_id, score in scores_snapshot.items():
        assert session.get(OperationData, operation_id).quality_score == score
    session.close()


def test_expired_lease_reclaim_rejects_stale_token(session_factory):
    """租约过期后单元可被重新领取，旧令牌提交被拒绝，不会二次写入。"""
    session = session_factory()
    seed(session, 2)
    session.close()

    clock = FakeClock()
    manager = RecalcManager(clock=clock, lease_seconds=60)
    session = session_factory()
    job = manager.create_job(session, name="租约回收", policy=make_policy(), batch_size=2)
    job_id = job.id

    stale = manager.claim_next_unit(session, job_id=job_id, worker_id="worker-a")
    clock.advance(seconds=120)
    reclaimed = manager.claim_next_unit(session, job_id=job_id, worker_id="worker-b")
    assert reclaimed["unit_id"] == stale["unit_id"]
    assert reclaimed["claim_token"] != stale["claim_token"]
    assert reclaimed["attempts"] == 2

    with pytest.raises(ClaimLostError):
        manager.complete_unit(session, job_id=job_id, unit_id=stale["unit_id"], claim_token=stale["claim_token"])

    outcome = manager.complete_unit(
        session, job_id=job_id, unit_id=reclaimed["unit_id"], claim_token=reclaimed["claim_token"]
    )
    assert outcome["success_count"] == 2
    progress = manager.job_progress(session, job_id)
    assert progress["completed_units"] == 1
    assert progress["success_items"] == 2
    session.close()


def test_policy_expiry_skips_remaining_units(session_factory):
    """策略失效：到期后剩余单元跳过并记录原因，已完成进度保留。"""
    session = session_factory()
    op_ids, _ = seed(session, 6)
    session.close()

    clock = FakeClock()
    expires_at = clock.moment + timedelta(hours=1)
    policy = make_policy(expires_at=expires_at)
    manager = RecalcManager(clock=clock)
    session = session_factory()
    job = manager.create_job(session, name="失效策略", policy=policy, batch_size=2)
    job_id = job.id

    first = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")
    manager.complete_unit(session, job_id=job_id, unit_id=first["unit_id"], claim_token=first["claim_token"])
    inflight = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")

    clock.advance(hours=2)
    with pytest.raises(PolicyExpiredError):
        manager.complete_unit(
            session, job_id=job_id, unit_id=inflight["unit_id"], claim_token=inflight["claim_token"]
        )
    assert manager.claim_next_unit(session, job_id=job_id, worker_id="w1") is None

    progress = manager.job_progress(session, job_id)
    assert progress["status"] == JOB_FAILED
    assert "策略已失效" in progress["error"]
    assert progress["completed_units"] == 1
    assert progress["skipped_units"] == 2
    assert progress["success_items"] == 2
    assert progress["skipped_items"] == 4

    units = manager.unit_results(session, job_id)
    assert units[0]["status"] == UNIT_COMPLETED
    assert all(unit["status"] == UNIT_SKIPPED and unit["reason"] == REASON_POLICY_EXPIRED for unit in units[1:])
    skipped_reasons = {
        item["reason"] for unit in units[1:] for item in unit["item_results"]
    }
    assert skipped_reasons == {REASON_POLICY_EXPIRED}

    done_ops = set(first["operation_ids"])
    assert done_ops <= scored_operation_ids(session)
    assert scored_operation_ids(session) == done_ops
    session.close()


def test_cancel_between_claim_and_complete_rejects_stale_token(session_factory):
    """取消竞态：取消落在领取与提交之间时，旧令牌提交被拒绝且不写入。"""
    session = session_factory()
    op_ids, _ = seed(session, 4)
    session.close()

    manager = RecalcManager(clock=FakeClock())
    session = session_factory()
    job = manager.create_job(session, name="取消竞态", policy=make_policy(), batch_size=2)
    job_id = job.id

    first = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")
    manager.complete_unit(session, job_id=job_id, unit_id=first["unit_id"], claim_token=first["claim_token"])
    second = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")

    manager.cancel_job(session, job_id=job_id)
    with pytest.raises(JobCancelledError):
        manager.complete_unit(session, job_id=job_id, unit_id=second["unit_id"], claim_token=second["claim_token"])

    progress = manager.job_progress(session, job_id)
    assert progress["status"] == JOB_CANCELLED
    assert progress["completed_units"] == 1
    assert progress["skipped_units"] == 1
    assert progress["success_items"] == 2
    assert progress["skipped_items"] == 2

    done_ops = set(first["operation_ids"])
    assert scored_operation_ids(session) == done_ops
    units = manager.unit_results(session, job_id)
    assert units[1]["status"] == UNIT_SKIPPED
    assert {item["reason"] for item in units[1]["item_results"]} == {REASON_JOB_CANCELLED}
    session.close()


def test_cancel_waits_for_inflight_completion_and_skips_rest(session_factory):
    """取消与进行中的提交竞争：提交完成后取消生效，其余单元跳过，状态一致。"""
    session = session_factory()
    op_ids, _ = seed(session, 4)
    session.close()

    entered = threading.Event()
    release = threading.Event()

    def blocking_scorer(operation, annotation, policy):
        if operation.id == op_ids[0]:
            entered.set()
            assert release.wait(timeout=5)
        return default_scorer(operation, annotation, policy)

    manager = RecalcManager(clock=FakeClock(), scorer=blocking_scorer)
    session = session_factory()
    job = manager.create_job(session, name="取消竞争", policy=make_policy(), batch_size=2)
    job_id = job.id

    worker_outcome = {}

    def worker():
        worker_session = session_factory()
        try:
            claim = manager.claim_next_unit(worker_session, job_id=job_id, worker_id="w1")
            worker_outcome["result"] = manager.complete_unit(
                worker_session, job_id=job_id, unit_id=claim["unit_id"], claim_token=claim["claim_token"]
            )
        finally:
            worker_session.close()

    worker_thread = threading.Thread(target=worker)
    worker_thread.start()
    assert entered.wait(timeout=5)

    cancel_result = {}

    def canceller():
        cancel_session = session_factory()
        try:
            cancel_result["job"] = manager.cancel_job(cancel_session, job_id=job_id)
        finally:
            cancel_session.close()

    cancel_thread = threading.Thread(target=canceller)
    cancel_thread.start()
    time.sleep(0.1)  # 让取消请求到达并等待提交完成
    release.set()
    worker_thread.join(timeout=5)
    cancel_thread.join(timeout=5)

    assert worker_outcome["result"]["status"] == UNIT_COMPLETED

    session = session_factory()
    progress = manager.job_progress(session, job_id)
    assert progress["status"] == JOB_CANCELLED
    assert progress["completed_units"] == 1
    assert progress["skipped_units"] == 1
    assert progress["success_items"] == 2
    assert progress["skipped_items"] == 2
    units = manager.unit_results(session, job_id)
    assert units[0]["status"] == UNIT_COMPLETED
    assert units[1]["status"] == UNIT_SKIPPED and units[1]["reason"] == REASON_JOB_CANCELLED
    assert scored_operation_ids(session) == set(op_ids[:2])
    session.close()


def test_item_failure_isolated_from_batch(session_factory):
    """单项失败隔离：一条记录评分异常不影响同批其他记录，原因可查询。"""
    session = session_factory()
    op_ids, _ = seed(session, 5)
    failing_id = op_ids[2]
    session.close()

    def flaky_scorer(operation, annotation, policy):
        if operation.id == failing_id:
            raise RuntimeError("评分引擎内部错误")
        return default_scorer(operation, annotation, policy)

    manager = RecalcManager(clock=FakeClock(), scorer=flaky_scorer)
    session = session_factory()
    job = manager.create_job(session, name="失败隔离", policy=make_policy(), batch_size=3)
    job_id = job.id
    session.close()

    assert drain_job(manager, session_factory, job_id) == 2

    session = session_factory()
    progress = manager.job_progress(session, job_id)
    assert progress["status"] == JOB_COMPLETED
    assert progress["success_items"] == 4
    assert progress["failed_items"] == 1

    units = manager.unit_results(session, job_id)
    failing_unit = units[0]
    assert failing_unit["status"] == UNIT_COMPLETED
    assert failing_unit["failed_count"] == 1
    assert failing_unit["success_count"] == 2
    failure = failing_unit["item_results"][0]
    assert failure["operation_id"] == failing_id
    assert failure["status"] == "failed"
    assert "评分引擎内部错误" in failure["reason"]

    assert session.get(OperationData, failing_id).quality_score is None
    assert scored_operation_ids(session) == set(op_ids) - {failing_id}
    session.close()


def test_restart_resumes_from_checkpoint(session_factory):
    """重启续跑：崩溃时领取中的单元被回收重发，已完成单元不会重做。"""
    session = session_factory()
    op_ids, _ = seed(session, 6)
    session.close()

    clock = FakeClock()
    manager_a = RecalcManager(clock=clock)
    session = session_factory()
    job = manager_a.create_job(session, name="重启续跑", policy=make_policy(), batch_size=2)
    job_id = job.id
    done = manager_a.claim_next_unit(session, job_id=job_id, worker_id="w1")
    manager_a.complete_unit(session, job_id=job_id, unit_id=done["unit_id"], claim_token=done["claim_token"])
    inflight = manager_a.claim_next_unit(session, job_id=job_id, worker_id="w1")
    session.close()

    # 模拟进程退出后再次启动：新的管理器实例从持久化检查点恢复
    manager_b = RecalcManager(clock=clock)
    session = session_factory()
    recovered = manager_b.recover(session)
    assert recovered == {"recovered_units": 1, "job_ids": [job_id]}

    reclaim = manager_b.claim_next_unit(session, job_id=job_id, worker_id="w2")
    assert reclaim["unit_id"] == inflight["unit_id"]
    assert reclaim["unit_index"] == 1
    manager_b.complete_unit(session, job_id=job_id, unit_id=reclaim["unit_id"], claim_token=reclaim["claim_token"])
    assert drain_job(manager_b, session_factory, job_id, worker_id="w2") == 1

    progress = manager_b.job_progress(session, job_id)
    assert progress["status"] == JOB_COMPLETED
    assert progress["completed_units"] == 3
    assert progress["success_items"] == 6

    units = manager_b.unit_results(session, job_id)
    assert units[0]["attempts"] == 1  # 已完成单元未被重新领取
    assert units[1]["attempts"] == 2  # 崩溃领取被回收后重发
    assert scored_operation_ids(session) == set(op_ids)
    session.close()


def test_pause_resume_preserves_progress(session_factory):
    """暂停时不再发放新单元，进行中的单元仍可完成，继续后正常领取。"""
    session = session_factory()
    seed(session, 4)
    session.close()

    manager = RecalcManager(clock=FakeClock())
    session = session_factory()
    job = manager.create_job(session, name="暂停继续", policy=make_policy(), batch_size=2)
    job_id = job.id

    inflight = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")
    manager.pause_job(session, job_id=job_id)
    assert manager.claim_next_unit(session, job_id=job_id, worker_id="w2") is None

    outcome = manager.complete_unit(
        session, job_id=job_id, unit_id=inflight["unit_id"], claim_token=inflight["claim_token"]
    )
    assert outcome["success_count"] == 2
    assert manager.claim_next_unit(session, job_id=job_id, worker_id="w2") is None

    manager.resume_job(session, job_id=job_id)
    assert drain_job(manager, session_factory, job_id) == 1
    progress = manager.job_progress(session, job_id)
    assert progress["status"] == JOB_COMPLETED
    assert progress["success_items"] == 4
    session.close()


def test_create_job_freezes_policy_and_filters(session_factory):
    """创建时冻结策略与筛选范围：后续新增数据与策略参数变化不影响本作业。"""
    session = session_factory()
    robot_model_id, scene_id, skill_id = ensure_base(session)
    full_ids = add_operations(session, robot_model_id, scene_id, skill_id, 2)
    partial_ids = add_operations(session, robot_model_id, scene_id, skill_id, 1, partial=True)
    session.close()

    manager = RecalcManager(clock=FakeClock())
    session = session_factory()
    policy = make_policy(completeness_weight=1.0, annotation_weight=0.0, grade_a_threshold=0.9)
    job = manager.create_job(
        session,
        name="冻结范围",
        policy=policy,
        filters={"robot_model_id": robot_model_id},
        batch_size=10,
    )
    job_id = job.id

    # 作业创建后新增匹配筛选条件的数据，不应进入本作业
    add_operations(session, robot_model_id, scene_id, skill_id, 3)

    progress = manager.job_progress(session, job_id)
    assert progress["total_items"] == 3
    assert progress["policy"]["completeness_weight"] == 1.0
    assert progress["policy"]["thresholds"]["grade_a"] == 0.9
    assert progress["filters"] == {"robot_model_id": robot_model_id}

    claim = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")
    assert claim["operation_ids"] == sorted(full_ids + partial_ids)
    manager.complete_unit(session, job_id=job_id, unit_id=claim["unit_id"], claim_token=claim["claim_token"])

    # 冻结策略按完整度满分权重计算：完整记录为 A，残缺记录按冻结阈值降级
    full_op = session.get(OperationData, full_ids[0])
    assert full_op.quality_score == full_op.completeness_score == 1.0
    assert full_op.data_grade == "A"
    partial_op = session.get(OperationData, partial_ids[0])
    assert partial_op.quality_score == partial_op.completeness_score
    assert partial_op.data_grade == "D"
    session.close()


def test_app_startup_recovers_inflight_claims(session_factory, monkeypatch):
    """应用启动钩子：进程重启后 lifespan 从持久化检查点恢复崩溃领取。"""
    from fastapi.testclient import TestClient

    import main

    session = session_factory()
    seed(session, 3)
    manager = RecalcManager(clock=FakeClock())
    job = manager.create_job(session, name="启动恢复", policy=make_policy(), batch_size=1)
    job_id = job.id
    claim = manager.claim_next_unit(session, job_id=job_id, worker_id="w1")
    assert claim is not None
    session.close()

    # 让启动钩子使用测试数据库，模拟进程退出后再次启动
    monkeypatch.setattr(main, "SessionLocal", session_factory)
    with TestClient(main.app):
        pass

    session = session_factory()
    units = manager.unit_results(session, job_id)
    assert all(unit["status"] == "pending" for unit in units)
    again = manager.claim_next_unit(session, job_id=job_id, worker_id="w2")
    assert again["unit_index"] == 0
    session.close()


def test_recalc_api_flow_and_online_grading_still_works(session_factory):
    """接口层：重算作业全流程可用，正常在线评分接口不受影响。"""
    from fastapi.testclient import TestClient

    from app.database import get_db
    from main import app

    def override_get_db():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    try:
        with TestClient(app) as client:
            session = session_factory()
            robot_model_id, scene_id, skill_id = ensure_base(session)
            add_operations(session, robot_model_id, scene_id, skill_id, 4)
            session.close()

            created = client.post(
                "/api/v1/recalc-jobs",
                json={"name": "接口重算", "batch_size": 2, "policy": {"completeness_weight": 0.5, "annotation_weight": 0.5}},
            )
            assert created.status_code == 200, created.text
            job_id = created.json()["job_id"]
            assert created.json()["total_units"] == 2

            while True:
                claim = client.post(f"/api/v1/recalc-jobs/{job_id}/claim", json={"worker_id": "api-worker"})
                assert claim.status_code == 200, claim.text
                unit = claim.json()["unit"]
                if unit is None:
                    break
                completed = client.post(
                    f"/api/v1/recalc-jobs/{job_id}/units/{unit['unit_id']}/complete",
                    json={"claim_token": unit["claim_token"]},
                )
                assert completed.status_code == 200, completed.text

            progress = client.get(f"/api/v1/recalc-jobs/{job_id}").json()
            assert progress["status"] == JOB_COMPLETED
            assert progress["success_items"] == 4

            units = client.get(f"/api/v1/recalc-jobs/{job_id}/units").json()
            assert len(units) == 2
            assert all(unit["status"] == UNIT_COMPLETED for unit in units)

            recovered = client.post("/api/v1/recalc-jobs/recover")
            assert recovered.status_code == 200
            assert recovered.json()["recovered_units"] == 0

            # 正常在线评分接口仍可使用
            online = client.post(
                "/api/v1/quality/grade-operations",
                json={"completeness_weight": 0.5, "annotation_weight": 0.5},
            )
            assert online.status_code == 200, online.text
            assert "已完成" in online.json()["message"]
    finally:
        app.dependency_overrides.clear()
