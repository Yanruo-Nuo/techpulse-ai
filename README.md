
<div align="center">

# TechPulse AI ⬛

**AI 数据工程平台** — 数据采集 · 流处理 · 多轮 AI 知识抽取 · 数仓建模 · 块级语义检索 · 全链路监控

[![Kafka](https://img.shields.io/badge/Message_Queue-Kafka-231F20?logo=apache-kafka)](https://kafka.apache.org/)
[![dbt](https://img.shields.io/badge/Data_Modeling-dbt-FF694B?logo=dbt)](https://www.getdbt.com/)
[![DashScope](https://img.shields.io/badge/AI_Enhancement-DashScope-1677FF)](https://dashscope.aliyun.com/)
[![Qdrant](https://img.shields.io/badge/Vector_DB-Qdrant-red)](https://qdrant.tech/)
[![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![Prometheus](https://img.shields.io/badge/Monitoring-Prometheus-E6522C?logo=prometheus)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Dashboard-Grafana-F46800?logo=grafana)](https://grafana.com/)
[![MaxCompute](https://img.shields.io/badge/Data_Warehouse-MaxCompute-FF6A00)](https://www.alibabacloud.com/product/maxcompute)

</div>

---

## 项目简介

TechPulse AI 从 6 个技术源（Hacker News、Reddit、GitHub Trending、Dev.to、Lobsters、RSS）实时采集技术新闻，经过 Kafka 流处理 → **多轮 AI 知识抽取** → OSS 数据湖 → MaxCompute 数仓 → dbt 分层建模 → Qdrant 块级语义检索 → Streamlit 前端展示的完整数据链路。

**AI 管线核心升级：** 当前项目将传统"单次 AI 摘要调用"升级为 **3 轮知识抽取管线**。文章经分块（段落级，512 tokens/块）→ 第一轮逐块实体提取 → 第二轮全局关联分析 → 第三轮整合推荐。产出结构化字段（技术实体、工具提及、应用场景），支持按块级检索精确定位具体段落。

**核心数据流：** `采集 → 消息队列 → 分块 → 多轮 AI 抽取 → 数据湖 → 数仓建模 → 块级向量检索 → 展示`

---

## 系统架构

---

## 运行指标（真实数据）

### 数据规模

| 指标 | 数值 | 说明 |
|------|------|------|
| 总文章数 | **125,801 行** | fact_article 总记录（含多版本） |
| 唯一文章 | **425 篇** | 去重后的独立文章数 |
| 数据时间跨度 | **25 天** | 2026-04-15 ~ 2026-05-10 |
| 活跃分区数 | **7 天** | 有数据写入的 ds 分区 |
| 日均采集 | **~500 篇/天** | 6 个爬虫综合产出 |
| 数据源数量 | **6 个** | HN / Reddit / GitHub / Dev.to / Lobsters / RSS |

### 数仓 & dbt

| 表 | 行数 | 说明 |
|----|------|------|
| `fact_article` | **125,801** | 事实表 |
| `int_article_enriched` | **125,801** | 窗口函数宽表 |
| `mart_daily_summary` | **167** | 每日 ROLLUP 汇总 |
| `mart_trend_analysis` | **462** | 分类趋势分析 |
| `dim_source` | **6** | 来源维度 |
| `dim_date` | **44** | 日期维度 |
| dbt 模型总数 | **7 个** | staging×1 + intermediate×1 + marts×5 |
| dbt 数据测试 | **42 个** | 32 PASS / 8 WARN / 2 ERROR |

### 向量检索

| 指标 | 数值 |
|------|------|
| Qdrant 向量数 | **425 条** |
| 向量维度 | 1536（DashScope text-embedding-v2）|
| 检索算法 | HNSW（O(log n)）|

### 监控

| 指标 | 详情 |
|------|------|
| Prometheus 端点 | 3 个（爬虫:8001 / 加工:8002 / 前端:8003）|
| Grafana 面板 | 9 个 |
| 告警规则 | 9 条（Critical 4 + Warning 5）|

---

```
采集层 ──push──→ Kafka ──poll──→ 流处理 ──→ AI 处理管线 ──→ OSS 数据湖
  6 scraper               batch=10    清洗+分类      │             Parquet
                                                    │                │
                                        ┌───────────┴──────────┐     │
                                        │    AI 处理管线 (v2)   │     │
                                        │                      │     │
                                        │  分块(chunker.py)     │     │
                                        │  ├─ HN: 按段落分割    │     │
                                        │  ├─ Reddit: 正文+评论 │     │
                                        │  └─ GitHub: 按章节    │     │
                                        │       ↓              │     │
                                        │ Round1: 逐块实体提取  │     │
                                        │  (tech_entities,     │     │
                                        │   tool_mentions,     │     │
                                        │   topic, difficulty) │     │
                                        │       ↓              │     │
                                        │ Round2: 关联分析     │     │
                                        │  (use_cases,         │     │
                                        │   related_tech,      │     │
                                        │   key_insights)      │     │
                                        │       ↓              │     │
                                        │ Round3: 整合推荐     │     │
                                        │  (tools_recommended, │     │
                                        │   my_project_         │     │
                                        │   relevance)         │     │
                                        └──────────────────────┘     │
                                                                    │
                                                       OSS → MaxCompute → dbt run → dbt test
                                                         每 300s 同步    42 个质量测试
                                                                    │
                                                            ┌───────┴───────┐
                                                            │               │
                                                     Streamlit          Qdrant (块级)
                                                   Timeline/KPI      HNSW O(log n)
                                                   AI 助手 RAG       payload 块级过滤
                                                                    支持段落级检索
                                                                    │
                                                            Prometheus + Grafana
                                                            3 端点 → 9 面板 → 9 告警规则
```

---

## 技术栈

### 数据管道

| 类别 | 技术 | 用途 |
|------|------|------|
| 消息队列 | Apache Kafka (KRaft) | 采集与加工解耦，单节点多源缓冲 |
| 流处理 | Python KafkaConsumer | 批量消费（batch=10），异常处理 + 死信 |
| 数据湖 | Alibaba OSS (Parquet) | 原始/加工数据存储，按 ds 分区 |
| 数据仓库 | Alibaba MaxCompute | MPP 数仓，分区表，每日增量 |
| 数据建模 | dbt (dbt-mc) | ODS → DWD → DWS → ADS 分层建模，版本控制 |
| 调度编排 | Bash + while True（规划 Airflow） | 定时同步 + dbt run |

### AI 管线（多轮知识抽取）

| 类别 | 技术 | 用途 |
|------|------|------|
| 文档分块 | `chunker.py`（自研）| 按来源类型路由（段落/Reddit/GitHub），512 tokens/块，overlap 64 |
| 第一轮提取 | DashScope / GLM-5.1 (temp=0.1) | 逐块提取技术实体、工具提及、主题、难度 |
| 第二轮关联 | DashScope / GLM-5.1 (temp=0.3) | 全文章关联分析，抽取应用场景、相关技术、核心观点 |
| 第三轮推荐 | DashScope / GLM-5.1 (temp=0.5) | 整合推荐：工具推荐 + 项目关联度评估 |
| 文本嵌入 | DashScope / text-embedding-v2 | 1536 维向量，每块独立 embedding |
| 向量数据库 | Qdrant | HNSW 近似检索，块级 payload 过滤 |
| 质量校验 | 自研 5 维度校验 | 缺失率/分类/JSON/幻觉 → Prometheus Gauge |

### 监控 & 基础设施

| 类别 | 技术 | 用途 |
|------|------|------|
| 指标采集 | Prometheus | 3 个 scrape 端点 (8001/8002/8003)，15s 间隔 |
| 可视化 | Grafana | 9 监控面板 + 9 告警规则 |
| 基础设施 | Docker Compose (7 容器) + Terraform | 本地部署 + 云资源管理 |
| 前端 | Streamlit | 3 页面：时间线 / 收藏 / AI 助手 |

---

## 数仓分层

| dbt 目录 | 分层 | 说明 | 物化 |
|----------|------|------|------|
| `sources.yml` | **ODS** | 原始数据接入 | 外部表引用 |
| `staging/` | **DWD** | 字段清洗、类型转换、分区裁剪 | view |
| `intermediate/` | **DWD** | **8 种窗口函数**特征衍生（SQL 面试考点）| table |
| `marts/` | **DWS/ADS** | 星型模型事实表 + ROLLUP + 滑动平均 | table |

### dbt 数据质量

| 校验项 | 数量 | 方式 |
|--------|------|------|
| 主键唯一性 | 6 个模型 | `unique` test |
| 非空约束 | 15+ 字段 | `not_null` test |
| 枚举值校验 | 5 组 | `accepted_values` test |
| 业务断言 | 5 条 | 自定义 singular test |
| **总计** | **42 个 test** | **32 PASS / 8 WARN / 2 ERROR** |

---

## 快速开始

### 环境要求

- Docker Engine 24+ & Docker Compose
- 4GB+ 可用 RAM
- 阿里云 DashScope API Key（试用可用）

### 启动

```bash
cd mage-ai

# 1. 配置环境变量
cp .env.example .env
# 编辑 .env 填入你的阿里云 AK / DashScope Key

# 2. 启动全部服务
docker compose up -d

# 3. 查看运行状态
docker compose ps
```

### 访问

| 服务 | 地址 | 说明 |
|------|------|------|
| Streamlit 前端 | http://localhost:8501 | 主界面 |
| Prometheus | http://localhost:9090 | 指标查询 |
| Grafana | http://localhost:3000 (admin/admin) | 监控面板 |
| Qdrant | http://localhost:6333/dashboard | 向量数据库 |

---

## 项目结构

```
techpulse-ai/
├── mage-ai/                          # 主项目
│   ├── docker-compose.yml            # 7 容器编排
│   ├── producer/                     # 6 个爬虫 + Kafka push
│   ├── techpulse_intelligence/       # 流处理 + AI + 质量 + 死信
│   │   ├── kafka_consumer.py         # 核心管道
│   │   ├── transformers/             # AI 增强 (billowing_hill.py)
│   │   ├── data_quality/             # 5 维度校验 + 死信队列
│   │   └── oss_to_mc_runner.py       # OSS → MaxCompute → dbt
│   ├── techpulse_dbt/                # dbt 数仓分层（7 模型 42 test）
│   ├── frontend/                     # Streamlit + Qdrant RAG
│   ├── prometheus/                   # 监控配置
│   ├── grafana/                      # 面板 + 告警
│   └── docs/                         # 指标体系 / 设计文档 / 学习清单
├── terraform/                        # 阿里云 OSS + 权限声明
└── README.md
```

---

## 项目亮点（面试可讲）

- **全链路数据工程**：采集 → Kafka → AI 增强 → 数据湖 → 数仓建模 → 服务展示 → 监控告警
- **多轮 AI 知识抽取**：分块 → 逐块实体提取 → 关联分析 → 整合推荐，替代传统单次摘要调用
- **块级语义检索**：Qdrant 按段落级索引，用户搜"Kafka partition"能命中长文中的具体段落而非整篇文章
- **结构化知识字段**：技术实体、工具提及、应用场景的独立 JSON 字段，可 SQL 检索："WHERE tech_entities LIKE '%Kafka%'"
- **dbt 分层建模**：ODS → DWD → DWS → ADS 完整分层，42 个数据质量测试
- **实时可观测性**：Prometheus 3 端点 + Grafana 9 面板 + 9 告警规则
- **成本意识**：Token 计费追踪、MaxCompute 扫描量监控

---

## 许可证

MIT
