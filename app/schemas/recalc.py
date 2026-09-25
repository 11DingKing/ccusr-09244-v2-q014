from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RecalcFilters(BaseModel):
    """创建重算作业时冻结的筛选范围。"""

    robot_model_id: Optional[int] = Field(None, description="机型ID过滤")
    scene_id: Optional[int] = Field(None, description="场景ID过滤")
    skill_id: Optional[int] = Field(None, description="技能ID过滤")
    data_grade: Optional[str] = Field(None, description="数据等级过滤")
    is_annotated: Optional[bool] = Field(None, description="是否已标注")


class RecalcPolicySpec(BaseModel):
    """创建重算作业时冻结的评分策略；失效时间需带时区，内部按 UTC 处理。"""

    completeness_weight: float = Field(0.5, ge=0, le=1, description="完整度权重")
    annotation_weight: float = Field(0.5, ge=0, le=1, description="标注质量权重")
    grade_a_threshold: float = Field(0.9, ge=0, le=1, description="A级阈值")
    grade_b_threshold: float = Field(0.7, ge=0, le=1, description="B级阈值")
    grade_c_threshold: float = Field(0.5, ge=0, le=1, description="C级阈值")
    expires_at: Optional[datetime] = Field(None, description="策略失效时间（需带时区），到期后剩余单元跳过")
    note: Optional[str] = Field(None, max_length=200, description="策略备注")


class RecalcJobCreate(BaseModel):
    name: str = Field(..., max_length=200, description="作业名称")
    created_by: Optional[str] = Field(None, max_length=100, description="创建人")
    filters: RecalcFilters = Field(default_factory=RecalcFilters, description="冻结的筛选范围")
    policy: RecalcPolicySpec = Field(default_factory=RecalcPolicySpec, description="冻结的评分策略")
    batch_size: int = Field(50, ge=1, le=500, description="每个工作单元的记录数")
    lease_seconds: int = Field(300, ge=1, le=86400, description="领取租约秒数，超时未提交可被回收")


class RecalcClaimRequest(BaseModel):
    worker_id: str = Field(..., max_length=100, description="领取者标识")


class RecalcCompleteRequest(BaseModel):
    claim_token: str = Field(..., description="领取时返回的令牌")


class RecalcFailRequest(BaseModel):
    claim_token: str = Field(..., description="领取时返回的令牌")
    reason: str = Field(..., max_length=500, description="失败原因")
