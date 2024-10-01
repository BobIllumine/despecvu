import json
import os
from models import Dataset

root = 'data'
js = []
for root, dir, files in os.walk(root):
    for file in files:
        awa: Dataset = {
            'record_id': file,
            'path': os.path.join(root, file)
        }
        if awa['record_id'].startswith('piracy'):
            continue
        js.append(awa)
js = js[:5]
with open('new_data.json', 'w+') as file:
    json.dump(js, file)