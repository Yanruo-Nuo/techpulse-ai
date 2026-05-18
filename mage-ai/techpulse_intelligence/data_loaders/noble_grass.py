@transformer
def transform_kafka_messages(messages, **kwargs):
    # messages 是从 curious_flower 流过来的 Kafka 原始消息列表
    import json
    import pandas as pd
    
    data = []
    for msg in messages:
        # 注意：取决于 Kafka 消息格式，有时 msg 已经是 dict，有时是 bytes
        try:
            if isinstance(msg, bytes):
                payload = json.loads(msg.decode('utf-8'))
            elif isinstance(msg, str):
                payload = json.loads(msg)
            else:
                payload = msg
            data.append(payload)
        except Exception as e:
            print(f"解析失败: {e}")
    
    return pd.DataFrame(data)