from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.recalc import (
    RecalcClaimRequest,
    RecalcCompleteRequest,
    RecalcFailRequest,
    RecalcJobCreate,
)
from app.services.recalc import (
    ClaimLostError,
    InvalidJobStateError,
    JobNotFoundError,
    PolicyExpiredError,
    RecalcError,
    RecalcValidationError,
    build_frozen_policy,
    recalc_manager,
)

router = APIRouter()


def _run(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except (ClaimLostError, InvalidJobStateError, PolicyExpiredError) as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RecalcValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RecalcError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/recalc-jobs", tags=["质量重算作业"])
def create_recalc_job(data: RecalcJobCreate, db: Session = Depends(get_db)):
    """创建重算作业：冻结策略与筛选范围，按作业数据 ID 升序切分工作单元。"""
    policy = _run(
        build_frozen_policy,
        completeness_weight=data.policy.completeness_weight,
        annotation_weight=data.policy.annotation_weight,
        grade_a_threshold=data.policy.grade_a_threshold,
        grade_b_threshold=data.policy.grade_b_threshold,
        grade_c_threshold=data.policy.grade_c_threshold,
        expires_at=data.policy.expires_at,
        note=data.policy.note or "",
    )
    job = _run(
        recalc_manager.create_job,
        db,
        name=data.name,
        policy=policy,
        filters=data.filters.model_dump(exclude_none=True),
        batch_size=data.batch_size,
        lease_seconds=data.lease_seconds,
        created_by=data.created_by,
    )
    return _run(recalc_manager.job_progress, db, job.id)


@router.get("/recalc-jobs", tags=["质量重算作业"])
def list_recalc_jobs(
    status: Optional[str] = Query(None, description="按作业状态过滤"),
    db: Session = Depends(get_db),
):
    return _run(recalc_manager.list_jobs, db, status)


@router.post("/recalc-jobs/recover", tags=["质量重算作业"])
def recover_recalc_jobs(db: Session = Depends(get_db)):
    """把进行中作业的崩溃领取重置为待领取（进程重启后的检查点恢复）。"""
    return _run(recalc_manager.recover, db)


@router.get("/recalc-jobs/{job_id}", tags=["质量重算作业"])
def get_recalc_job(job_id: int, db: Session = Depends(get_db)):
    return _run(recalc_manager.job_progress, db, job_id)


@router.get("/recalc-jobs/{job_id}/units", tags=["质量重算作业"])
def get_recalc_job_units(job_id: int, db: Session = Depends(get_db)):
    """按批返回成功、跳过、失败数量及单项原因。"""
    return _run(recalc_manager.unit_results, db, job_id)


@router.post("/recalc-jobs/{job_id}/claim", tags=["质量重算作业"])
def claim_recalc_unit(job_id: int, data: RecalcClaimRequest, db: Session = Depends(get_db)):
    """按稳定顺序领取下一个工作单元；无可领取单元时 unit 为 null。"""
    unit = _run(recalc_manager.claim_next_unit, db, job_id=job_id, worker_id=data.worker_id)
    job = _run(recalc_manager.get_job, db, job_id)
    return {"job_id": job_id, "job_status": job.status, "unit": unit}


@router.post("/recalc-jobs/{job_id}/units/{unit_id}/complete", tags=["质量重算作业"])
def complete_recalc_unit(job_id: int, unit_id: int, data: RecalcCompleteRequest, db: Session = Depends(get_db)):
    """提交已领取单元：逐项重算写入，单项失败隔离，重复提交幂等。"""
    return _run(recalc_manager.complete_unit, db, job_id=job_id, unit_id=unit_id, claim_token=data.claim_token)


@router.post("/recalc-jobs/{job_id}/units/{unit_id}/fail", tags=["质量重算作业"])
def fail_recalc_unit(job_id: int, unit_id: int, data: RecalcFailRequest, db: Session = Depends(get_db)):
    return _run(
        recalc_manager.fail_unit,
        db,
        job_id=job_id,
        unit_id=unit_id,
        claim_token=data.claim_token,
        reason=data.reason,
    )


@router.post("/recalc-jobs/{job_id}/pause", tags=["质量重算作业"])
def pause_recalc_job(job_id: int, db: Session = Depends(get_db)):
    job = _run(recalc_manager.pause_job, db, job_id=job_id)
    return _run(recalc_manager.job_progress, db, job.id)


@router.post("/recalc-jobs/{job_id}/resume", tags=["质量重算作业"])
def resume_recalc_job(job_id: int, db: Session = Depends(get_db)):
    job = _run(recalc_manager.resume_job, db, job_id=job_id)
    return _run(recalc_manager.job_progress, db, job.id)


@router.post("/recalc-jobs/{job_id}/cancel", tags=["质量重算作业"])
def cancel_recalc_job(job_id: int, db: Session = Depends(get_db)):
    """取消作业：已完成进度保留，未完成单元标记跳过。"""
    job = _run(recalc_manager.cancel_job, db, job_id=job_id)
    return _run(recalc_manager.job_progress, db, job.id)
