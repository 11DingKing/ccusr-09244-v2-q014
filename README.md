# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册；启动时从持久化检查点恢复重算作业。
- `app/models`：业务实体及其关系。
- `app/routers`：基础资源、作业、数据集、分析和质量重算接口。
- `app/services`：评分、统计、策略目录、时间窗口与历史数据重算编排。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 历史数据质量重算

评分策略调整后，可在业务低峰通过 `POST /api/v1/recalc-jobs` 创建重算作业：

- 创建时冻结策略参数（权重、分级阈值、失效时间）与筛选范围，后续变化不影响本作业；
- 工作单元按作业数据 ID 升序切批，工作协程通过 `POST /recalc-jobs/{id}/claim` 按稳定顺序领取，重复领取不会二次写入；
- 提交结果（`POST /recalc-jobs/{id}/units/{unit_id}/complete`）需携带领取令牌，单项失败只记录原因并继续；
- `POST /recalc-jobs/{id}/pause|resume|cancel` 暂停、继续、取消，已完成进度均保留；
- 进程重启后自动从检查点恢复（也可手动 `POST /recalc-jobs/recover`），崩溃时领取中的单元重新发放；
- `GET /recalc-jobs/{id}` 查看进度，`GET /recalc-jobs/{id}/units` 按批查询成功、跳过和失败原因。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。
