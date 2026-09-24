# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册。
- `app/models`：业务实体及其关系。
- `app/routers`：基础资源、作业、数据集和分析接口。
- `app/services`：评分、统计、策略目录、时间窗口工具与重算作业编排。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 历史作业重算编排

评分策略调整后，可用进程内作业编排分批重算历史作业，替代一次性同步处理全部记录的
`POST /api/v1/quality/grade-operations`（该在线接口行为保持不变）：

1. `POST /api/v1/quality/policies` 发布带版本号与指纹的评分策略；策略发布后不可变，
   后续可通过 `/quality/policies/{revision}/deactivate` 作废。
2. `POST /api/v1/quality/recalculation-jobs` 创建作业：创建时冻结策略版本与筛选范围
   （机型/场景/技能/序列号/当前等级），范围内的作业按 ID 物化为有序工作单元，
   之后发布新策略或新增数据都不影响在跑作业。
3. 工作单元按稳定顺序领取（`.../claim`，返回领取凭证 token），处理后通过
   `.../batches/{batch_id}/report` 回报；也可直接调用 `.../run-batch` 由服务端
   领取、按冻结策略重算并回报，适合低峰期串行回填。
4. 支持 `.../pause`、`.../resume`、`.../cancel`：暂停后不再发放新批次但已领取批次
   可正常回报；取消后在途批次的迟到回报一律拒绝；已完成进度始终保留。
5. 所有状态（作业、工作单元、批次、单项结果）持久化在业务数据库中，进程重启后
   新编排器直接从检查点继续；超过租约（默认 30 分钟）未回报的领取会被其他批次
   接管，旧持有者的回报凭失效 token 会被整批拒绝，重复领取不会二次写入。
6. 通过 `GET .../{job_id}` 查看总/成功/跳过/失败计数与进度，
   `GET .../{job_id}/batches` 查看每批成功、跳过、失败数量，
   `GET .../{job_id}/outcomes?result=failed` 查询单项失败原因（支持按批次过滤）。

编排器的时钟与租约时长可注入（`RecalculationOrchestrator(session_factory, now=..., lease_timeout=...)`），
便于稳定验证时间相关行为。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。
