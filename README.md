# COMP5339 Assignment 1 — EV Charger Data Integration

A/B/C 已提供实现及交接 CSV；D 已补充建库脚本、Notebook、DDL、数据库设计及质量检查。数据库的实际运行结果见 `data/final/quality_report.json`。

**这是 D 集成版本，不是已完成全部提交审核的最终课程提交。** C 原始 API 缓存/候选复核文件尚未随原 ZIP 提供，团队报告与全组 AI 使用报告仍需完成，另一成员需独立复现。没有把这些未完成事项标为已通过。

## 1. 项目结构

| 路径 | 内容 |
| --- | --- |
| `notebooks/01 acquire_clean.ipynb` | A：自动获取及清洗（文件名包含空格） |
| `notebooks/02_spatial.ipynb` | B：空间整合 |
| `notebooks/03_augment.ipynb` | C：Open Charge Map 增强 |
| `notebooks/04_database.ipynb` | D：建库、验证、查询和交接 |
| `scripts/build_database.py` | 与 D Notebook 共用的建库实现 |
| `sql/schema.sql` | 在空数据库中重建结构的独立 DDL |
| `data/raw/ev_20251216.csv` | 2025 年 12 月官方原始数据 |
| `data/raw/SA4_2026_AUST_SHP_GDA2020.zip` | 完整澳大利亚 SA4 边界及配套文件 |
| `data/processed/chargers_clean.csv` | A：1,958 条清洗后源记录 |
| `data/processed/charger_sa4.csv` | B：1,958 条空间结果 |
| `data/processed/charger_sa4_review.csv` | B：1 条未解决候选区域 |
| `data/processed/charger_attributes.csv` | C：216 条接受的增强匹配 |
| `data/final/project.duckdb` | D：最终数据库 |
| `data/final/quality_report.json` | 质量数字、版本、指纹与已知局限 |
| `data/final/sa4_summary.csv` | 按 SA4 汇总的源记录数量 |
| `data/final/augmentation_distance_review.csv` | 超过 3 km 的增强匹配复核清单 |
| `docs/database_design.md` | 数据库关系图、设计理由与空间说明 |
| `docs/D_handoff.md` | D 完成内容、验证和团队后续事项 |
| `docs/AI_usage_D.md` | 本次 AI 使用记录，供团队核实并汇总 |
| `tests/test_database.py` | D 的关系约束与错误输入保护检查 |
| `output/pdf/` | 原始作业说明及中文参考译文 |

原 ZIP 的中文 PDF 文件名无法在本机正常解压，已保留其原始字节并重命名为 `COMP5339_2026s2_Assignment1_Chinese.pdf`。

## 2. 环境与快速运行 D

在项目根目录打开终端。验证环境为 Python 3.11；依赖精确版本写在 requirements.txt。

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/build_database.py
```

Windows 激活：`.venv\Scripts\activate`。脚本根据自身位置确定项目根目录，也可显式传 `--root PATH`。

或启动 `python -m jupyterlab`，选择该环境的 Python 内核，打开 `notebooks/04_database.ipynb`，重启内核并运行全部单元格。Notebook 和命令行会重建同一结果，无需先重跑 A/B/C。

首次运行会安装官方 DuckDB spatial 扩展到项目 `.duckdb_extensions/`，需要网络；之后本机复用缓存。扩展按平台/版本区分，ZIP 不打包机器专用缓存。请使用 requirements.txt 指定的 DuckDB 版本打开数据库。

## 3. 完整流程重跑

1. 在 Jupyter 中打开 `notebooks/01 acquire_clean.ipynb`，确保其内核工作目录是 `notebooks/`（A 当前使用 `Path.cwd().parent`）。它下载原始 CSV 和边界并生成清洗数据。
2. 运行 `02_spatial.ipynb` 和 `03_augment.ipynb`。B 需要 A 解压得到的 `data/raw/sa4_2026/`；D 自行从 ZIP 临时解压，不依赖该目录。
3. C 读取环境变量 `OCM_API_KEY`，或复用 `data/external/ocm_pilot_raw.json` 和 `ocm_australia_raw.json`。来源为 [Open Charge Map API](https://api.openchargemap.io/v3/poi/)，具体参数、规则和复核逻辑见 C Notebook。不要将密钥写入文件或提交包。
4. 运行 D。输入文件变化时须重跑相关下游，并重新核对报告数字。
5. 另一成员在独立环境复现。

**当前缺件：** `ocm_pilot_raw.json`、`ocm_australia_raw.json`、`ocm_pilot_candidate_review.csv`、`ocm_full_candidate_review.csv`。请从 C 获取生成这版 CSV 时的对应副本。重新访问 API 会得到新的时点数据，不能冒充原始获取副本。当前没有宣称完整 A→B/C→D 链路已从头复现。

官方原始数据来源与程序化下载地址保留在 A Notebook；获取日期/增强数据获取时间不能由文件修改时间或外部验证日期代替。

## 4. 接口与字段约定

- 使用磁盘文件和 `record_id` 关联，不能使用行号。`record_id` 是 A 对原始记录内容生成的稳定散列，不是唯一物理站点。
- A：`Latitude`、`Longitude` 为数值；`Number_of_plugs` 为正整数；`power_kw` 可缺失；邮编、OBJECTID、record_id 保留为文本；质量标记为布尔值。全部字段原名入库。
- `operator_standardized` 关联运营商表；原始运营商标签仍保存。
- B：`SA4_CODE26` 为文本，可 NULL；每条源记录一条结果。无匹配的行仍保留，候选区域只用于复核。
- C：UTF-8 BOM 自动处理；来源/外部 ID/record_id 形成联合主键。插头类型拆分为子表，但不推断插头与功率列表的一一对应关系。
- CSV 空字段转 NULL；不把未知值填成零。新增数据若违反键关系、数值类型或上下游一致性会报错，旧数据库保留。
- 点几何为假设的 EPSG:4326，区域为已确认 EPSG:7844；空间 SQL 显式转换，B 的 CRS 未确认状态保留。

## 5. 数据质量与验收

| 指标 | 当前交接数据 |
| --- | ---: |
| 基础源记录 | 1,958 |
| 有 SA4 / 无 SA4 | 1,957 / 1 |
| DC 源记录 / 六位小数去重位置 | 433 / 430 |
| 有增强 DC 源记录 / 位置 | 216 / 215 |
| 源记录口径覆盖率 | 49.88% |
| C 的位置口径覆盖率 | 50.00% |
| 超过 3 km 的增强匹配 | 17 |

新增属性依据 C 的 connector_types、external_connection_quantity、external_number_of_points、external_status、external_usage_type、external_usage_cost 至少一个非空判断。口径沿用 C，不把两个覆盖率混写。

D 检查 A/B 记录集合与字段一致性、主外键、导入数量、几何有效性，并独立用 DuckDB within 空间连接核对 B 的全部结果。该检查不证明源坐标与地址一致，也不代表重新确认 C 的外部匹配。

运行 D 的检查：

```bash
python -m unittest discover -s tests -v
```

数据库使用示例（在项目根目录的 Python 环境）：

```python
from pathlib import Path
from scripts.build_database import connect_database
with connect_database(Path.cwd()) as con:
    print(con.execute("SELECT * FROM sa4_summary WHERE source_records > 0").df())
    print(con.execute("SELECT * FROM augmentation_review").df())
```

独立 DDL：新建 DuckDB 连接，先 `INSTALL spatial; LOAD spatial;`，再执行 `sql/schema.sql`。DDL 只建结构，数据加载由建库脚本完成。关系图及设计理由见 `docs/database_design.md`。

## 6. 提交前

- C 补齐原始缓存与候选复核文件，团队核对 17 条远距离匹配、重复外部 ID 和覆盖率口径。
- A/B 确认 CRS 假设和 1 条未匹配记录；不根据最近候选直接补填 SA4。
- C 复核 D；A/另一成员在干净环境复现完整流程。
- 团队完成最多 6 页正文的项目报告、成员信息/贡献及全组 AI 使用报告。本次提供 D 的设计与使用记录，未生成或冒充全组正式报告。
- ZIP 应包含代码、DDL、数据库、原始与处理数据、增强获取副本及说明；Git 忽略文件不会自动进入仓库导出 ZIP。核对实际文件后由 A 上传。
