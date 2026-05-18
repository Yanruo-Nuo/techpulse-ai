FROM python:3.10-slim

WORKDIR /app

# 1. 安装系统工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. 升级 pip 并安装所有依赖
# 增加超时时间以防止网络波动
RUN pip install --no-cache-dir --default-timeout=1000 \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    streamlit pyodps pandas plotly dashscope prometheus-client qdrant-client

COPY . .

EXPOSE 8501
EXPOSE 8003

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
