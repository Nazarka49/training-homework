from typing import List, Dict, Any
import os
import json


def safe_open_w(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, 'w+')


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    file_name = 'sales_' + os.path.basename(path)
    completeName = os.path.join(path, file_name + ".json")
    with safe_open_w(completeName) as f:
        json.dump(json_content, f)
    print('done')
    pass

