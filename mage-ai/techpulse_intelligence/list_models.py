import os, json, urllib.request
api_key = os.environ['DASHSCOPE_KEY']
page = 1
found = []
while page <= 5:
    url = f'https://dashscope.aliyuncs.com/api/v1/models?page_no={page}&page_size=100'
    req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + api_key})
    resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
    models = resp.get('output', {}).get('models', [])
    if not models:
        break
    for m in models:
        mid = m.get('model', '')
        name = m.get('name', '')
        if '3.6' in mid or '3.6' in name:
            found.append((mid, name))
        if 'qwen' in mid.lower() and ('plus' in mid.lower() or 'max' in mid.lower()):
            found.append((mid, name))
    page += 1
print(f'Found {len(found)} matching models:')
for mid, name in found:
    print(f'  {mid:30s} {name}')
