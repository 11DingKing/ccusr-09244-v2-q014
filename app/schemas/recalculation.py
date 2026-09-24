"""重算编排接口的请求/响应模型。"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class QualityPolicyCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="策略名称")
    completeness_weight: float = Field(0.5, ge=0, le=1, description="完整度权重")
    annotation_weight: float = Field(0.5, ge=0, le=1, description="标注质量权重")
    grade_a_threshold: float = Field(0.9, ge=0, le=1, description="A级阈值")
    grade_b_threshold: float = Field(0.7, ge=0, le=1, description="B级阈值")
    grade_c_threshold: float = Field(0.5, ge=0, le=1, description="C级阈值")
    revision: Optional[int] = Field(None, ge=1, description="指定版本号，默认自增")


class QualityPolicyResponse(BaseModel):
    revision: int
    name: str
    completeness_weight: float
    annotation_weight: float
    grade_a_threshold: float
    grade_b_threshold: float
    grade_c_threshold: float
    fingerprint: str
    is_active: bool
    created_at: Optional[datetime] = None


class RecalculationJobCreate(BaseModel):
    policy_revision: Optional[int] = Field(None, description="冻结使用的策略版本，默认最新生效版本")
    robot_model_id: Optional[int] = Field(None, description="机型ID筛选范围")
    scene_id: Optional[int] = Field(None, description="场景ID筛选范围")
    skill_id: Optional[int] = Field(None, description="技能ID筛选范围")
    robot_serial: Optional[str] = Field(None, max_length=100, description="机器人序列号筛选范围")
    data_grade: Optional[str] = Field(None, max_length=10, description="当前数据等级筛选范围")
    created_by: Optional[str] = Field(None, max_length=100, description="创建人")


class FrozenPolicyView(BaseModel):
    revision: int
    name: str
    completeness_weight: float
    annotation_weight: float
    grade_a_threshold: float
    grade_b_threshold: float
    grade_c_threshold: float
    fingerprint: str


class RecalculationJobResponse(BaseModel):
    id: int
    status: str
    total_count: int
    claimed_count: int
    succeeded_count: int
    skipped_count: int
    failed_count: int
    finished_count: int
    remaining_count: int
    progress: float
    last_processed_id: Optional[int] = None
    error: Optional[str] = None
    scope: dict
    created_by: Optional[str] = None
    policy: FrozenPolicyView
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    paused_at: Optional[datetime] = None
    resumed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None


class ClaimRequest(BaseModel):
    batch_size: int = Field(50, ge=1, le=500, description="每批工作单元数量")
    claimed_by: Optional[str] = Field(None, max_length=100, description="领取者标识")


class ClaimResponse(BaseModel):
    job_id: int
    batch_id: Optional[int] = None
    sequence: int
    token: Optional[str] = Field(None, description="回报时必须携带的领取凭证")
    claimed_by: Optional[str] = None
    operation_ids: List[int]
    empty: bool = Field(False, description="是否没有可领取单元")


class OutcomeItem(BaseModel):
    operation_id: int
    result: str = Field(..., description="success / skipped / failed")
    reason: Optional[str] = Field(None, description="跳过或失败原因")
    completeness_score: Optional[float] = None
    quality_score: Optional[float] = None
    before_grade: Optional[str] = None
    after_grade: Optional[str] = None


class ReportRequest(BaseModel):
    token: str = Field(..., description="领取时返回的凭证")
    outcomes: List[OutcomeItem] = Field(..., description="与领取单元一一对应的结果")


class BatchResponse(BaseModel):
    id: int
    job_id: int
    sequence: int
    status: str
    claimed_by: Optional[str] = None
    outcome_count: int
    success_count: int
    skipped_count: int
    failed_count: int
    created_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ReportResponse(BaseModel):
    batch: BatchResponse
    job_status: str
    job: RecalculationJobResponse


class OutcomeResponse(BaseModel):
    batch_id: int
    operation_id: int
    result: str
    reason: Optional[str] = None
    before_grade: Optional[str] = None
    after_grade: Optional[str] = None
    recorded_at: Optional[datetime] = None
