import os
from odps import ODPS
from dashscope import TextEmbedding
from dashvector import Client, Doc

# 初始化组件
o = ODPS(os.getenv('AK'), os.getenv('SK'), project='techpulse_dw', endpoint='...')
dv_client = Client(endpoint=os.getenv('DV_ENDPOINT'), api_key=os.getenv('DV_API_KEY'))
collection = dv_client.get('tech_news_v1')

# 1. 从 MaxCompute 读取最近 100 条洞察
sql = "SELECT title, ai_insight FROM default_staging.stg_tech_news LIMIT 100"
with o.execute_sql(sql).open_reader() as reader:
    for row in reader:
        # 2. 生成向量
        resp = TextEmbedding.call(model=TextEmbedding.Models.text_embedding_v2, input=row.ai_insight)
        embedding = resp.output['embeddings'][0]['embedding']
        
        # 3. 存入向量库
        collection.insert(Doc(id=row.title, vector=embedding, fields={"content": row.ai_insight}))

print("✅ 向量数据库同步完成！现在 AI 拥有‘短期记忆’了。")
