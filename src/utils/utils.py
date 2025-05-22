import os
import json
import sys

ODQA_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ODQA_ROOT_PATH)


def load_data(directory):
    data = []
    files = os.listdir(directory)
    for file in files:
        with open(os.path.join(directory, file), 'r', encoding='utf-8') as f:
            month_data = json.load(f)
            data.extend(month_data)
    return data