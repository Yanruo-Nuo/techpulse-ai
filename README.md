# TechPulse AI

> **智能技术新闻数据平台** — 包含数据采集、流处理、AI 增强、数仓建模、向量检索与监控告警的数据工程项目。

本仓库的主要项目代码位于 `mage-ai/` 目录下。跳转查看完整文档：

➡️ **[mage-ai/README.md](mage-ai/README.md)**

---

## 目录结构

```
/
├── mage-ai/                  ← 主项目代码
│   ├── docker-compose.yml    ← 7 容器编排
│   ├── producer/             ← 多源采集器
│   ├── techpulse_intelligence/ ← 流处理 + AI + 质量
│   ├── techpulse_dbt/        ← dbt 数仓建模
│   ├── frontend/             ← Streamlit 前端
│   ├── prometheus/           ← 监控配置
│   ├── grafana/              ← 仪表盘 + 告警
│   └── docs/                 ← 设计文档
├── terraform/                ← 阿里云基础设施
└── README.md                 ← 本文档
```
