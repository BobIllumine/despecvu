import json
import os
from src.pocketbase import PBManager
from src.recognizer import Recognizer
from models import Dataset


if __name__ == '__main__':
    pbman = PBManager()
    pbman.set_collections('fingerprints', 'records')
    # pbman.delete_all('fingerprints')
    # pbman.delete_all('records')
    recognizer = Recognizer(pbman)

    path = 'new_data.json'
    with open(path) as meta:
        dataset: list[Dataset] = [Dataset(**data) for data in json.load(meta)]

    for file in dataset:
        file.path = os.path.join(os.path.dirname(__file__), file.path)
    # recognizer.upload_data(datasest)

    test_file = os.path.join(os.path.dirname(__file__), 'data/piracy_5.wav')
    results = recognizer.recognize_file(test_file)
    print(results)
