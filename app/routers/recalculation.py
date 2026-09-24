"""历史作业重算编排接口。

在线评分仍使用 analytics 中的 /quality/grade-operations；
本路由提供可暂停、可恢复、可查询批次结果的批量重算作业。
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.database import SessionLocal
from app.schemas.recalculation import (
    ClaimRequest,
    ClaimResponse,
    OutcomeItem,
    QualityPolicyCreate,
    QualityPolicyResponse,
    RecalculationJobCreate,
    RecalculationJobResponse,
    ReportRequest,
    ReportResponse,
    BatchResponse,
    OutcomeResponse,
)
from app.services.recalculation import (
    ClaimLostError,
    EmptyScopeError,
    JobNotFoundError,
    JobStateError,
    PolicyStaleError,
    RecalculationError,
    RecalculationOrchestrator,
)

router = APIRouter()


def get_orchestrator() -> RecalculationOrchestrator:
    # 编排器自身无状态可变数据，进度全部在数据库中，单例即可。
    return RecalculationOrchestrator(SessionLocal)


def _raise_orchestration_error(exc: RecalculationError) -> None:
    if isinstance(exc, JobNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, (JobStateError, PolicyStaleError, ClaimLostError)):
        raise HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, EmptyScopeError):
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=400, detail=str(exc))


# ---------------------------------------------------------------- 策略版本


@router.post(
    "/quality/policies",
    response_model=QualityPolicyResponse,
    tags=["质量策略"],
)
def publish_quality_policy(
    data: QualityPolicyCreate,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        policy = orchestrator.publish_policy(
            name=data.name,
            completeness_weight=data.completeness_weight,
            annotation_weight=data.annotation_weight,
            grade_a_threshold=data.grade_a_threshold,
            grade_b_threshold=data.grade_b_threshold,
            grade_c_threshold=data.grade_c_threshold,
            revision=data.revision,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    for item in orchestrator.list_policies():
        if item["revision"] == policy.revision:
            return item
    return {**policy.as_dict(), "is_active": True, "created_at": None}


@router.get(
    "/quality/policies",
    response_model=List[QualityPolicyResponse],
    tags=["质量策略"],
)
def list_quality_policies(
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    return orchestrator.list_policies()


@router.post(
    "/quality/policies/{revision}/deactivate",
    response_model=QualityPolicyResponse,
    tags=["质量策略"],
)
def deactivate_quality_policy(
    revision: int,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        orchestrator.deactivate_policy(revision)
        for item in orchestrator.list_policies():
            if item["revision"] == revision:
                return item
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


# ---------------------------------------------------------------- 作业管理


@router.post(
    "/quality/recalculation-jobs",
    response_model=RecalculationJobResponse,
    tags=["重算作业"],
)
def create_recalculation_job(
    data: RecalculationJobCreate,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        return orchestrator.create_job(
            scope=data.model_dump(exclude={"policy_revision", "created_by"}),
            policy_revision=data.policy_revision,
            created_by=data.created_by,
        )
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


@router.get(
    "/quality/recalculation-jobs",
    response_model=List[RecalculationJobResponse],
    tags=["重算作业"],
)
def list_recalculation_jobs(
    status: Optional[str] = Query(None, description="按作业状态过滤"),
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    return orchestrator.list_jobs(status=status)


@router.get(
    "/quality/recalculation-jobs/{job_id}",
    response_model=RecalculationJobResponse,
    tags=["重算作业"],
)
def get_recalculation_job(
    job_id: int,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        return orchestrator.get_job(job_id)
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


@router.post(
    "/quality/recalculation-jobs/{job_id}/pause",
    response_model=RecalculationJobResponse,
    tags=["重算作业"],
)
def pause_recalculation_job(
    job_id: int,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        return orchestrator.pause_job(job_id)
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


@router.post(
    "/quality/recalculation-jobs/{job_id}/resume",
    response_model=RecalculationJobResponse,
    tags=["重算作业"],
)
def resume_recalculation_job(
    job_id: int,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        return orchestrator.resume_job(job_id)
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


@router.post(
    "/quality/recalculation-jobs/{job_id}/cancel",
    response_model=RecalculationJobResponse,
    tags=["重算作业"],
)
def cancel_recalculation_job(
    job_id: int,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        return orchestrator.cancel_job(job_id)
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


# ------------------------------------------------------------ 领取与回报


@router.post(
    "/quality/recalculation-jobs/{job_id}/claim",
    response_model=ClaimResponse,
    tags=["重算作业"],
)
def claim_recalculation_batch(
    job_id: int,
    data: ClaimRequest,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        claim = orchestrator.claim_batch(
            job_id, batch_size=data.batch_size, claimed_by=data.claimed_by
        )
    except RecalculationError as exc:
        _raise_orchestration_error(exc)
    return {
        "job_id": claim.job_id,
        "batch_id": claim.batch_id if claim.operation_ids else None,
        "sequence": claim.sequence,
        "token": claim.token or None,
        "claimed_by": claim.claimed_by,
        "operation_ids": claim.operation_ids,
        "empty": not claim.operation_ids,
    }


@router.post(
    "/quality/recalculation-jobs/{job_id}/batches/{batch_id}/report",
    response_model=ReportResponse,
    tags=["重算作业"],
)
def report_recalculation_batch(
    job_id: int,
    batch_id: int,
    data: ReportRequest,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    from app.services.recalculation import UnitOutcome

    outcomes = []
    for item in data.outcomes:
        if item.result not in ("success", "skipped", "failed"):
            raise HTTPException(status_code=400, detail=f"未知结果类型: {item.result}")
        outcomes.append(
            UnitOutcome(
                operation_id=item.operation_id,
                result=item.result,
                reason=item.reason,
                completeness_score=item.completeness_score,
                quality_score=item.quality_score,
                before_grade=item.before_grade,
                after_grade=item.after_grade,
            )
        )
    try:
        return orchestrator.report_batch(job_id, batch_id, data.token, outcomes)
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


@router.post(
    "/quality/recalculation-jobs/{job_id}/run-batch",
    tags=["重算作业"],
)
def run_recalculation_batch(
    job_id: int,
    batch_size: int = Query(50, ge=1, le=500),
    claimed_by: Optional[str] = Query(None),
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    """便捷接口：服务端领取一批、按冻结策略重算并回报，供低峰期串行回填。"""

    try:
        result = orchestrator.run_batch(
            job_id, batch_size=batch_size, claimed_by=claimed_by
        )
    except RecalculationError as exc:
        _raise_orchestration_error(exc)
    if result is None:
        return {"empty": True, "outcomes": []}
    job = orchestrator.get_job(job_id)
    return {
        "empty": False,
        "batch_id": result.claim.batch_id,
        "sequence": result.claim.sequence,
        "finished": result.finished,
        "success_count": result.success_count,
        "skipped_count": result.skipped_count,
        "failed_count": result.failed_count,
        "outcomes": [
            {
                "operation_id": item.operation_id,
                "result": item.result,
                "reason": item.reason,
                "before_grade": item.before_grade,
                "after_grade": item.after_grade,
            }
            for item in result.outcomes
        ],
        "job": job,
    }


# ---------------------------------------------------------------- 结果查询


@router.get(
    "/quality/recalculation-jobs/{job_id}/batches",
    response_model=List[BatchResponse],
    tags=["重算作业"],
)
def list_recalculation_batches(
    job_id: int,
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    try:
        return orchestrator.list_batches(job_id)
    except RecalculationError as exc:
        _raise_orchestration_error(exc)


@router.get(
    "/quality/recalculation-jobs/{job_id}/outcomes",
    response_model=List[OutcomeResponse],
    tags=["重算作业"],
)
def list_recalculation_outcomes(
    job_id: int,
    batch_id: Optional[int] = Query(None, description="限定批次"),
    result: Optional[str] = Query(None, description="success / skipped / failed"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    orchestrator: RecalculationOrchestrator = Depends(get_orchestrator),
):
    if result is not None and result not in ("success", "skipped", "failed"):
        raise HTTPException(status_code=400, detail="result 只能是 success/skipped/failed")
    try:
        return orchestrator.list_outcomes(
            job_id, batch_id=batch_id, result=result, limit=limit, offset=offset
        )
    except RecalculationError as exc:
        _raise_orchestration_error(exc)
