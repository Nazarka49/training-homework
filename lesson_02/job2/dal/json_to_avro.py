from fastavro import parse_schema, writer
from typing import List, Dict, Any
import json
import os
schema = {
    #'name': 'sales_2022-08-09',
    'name': 'file',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'float'},
    ]
}
parsed_schema = parse_schema(schema)
def get_json(path: str) -> json:
    file_name = 'sales_' + os.path.basename(path)
    complete_name = os.path.join(path, file_name + ".json")
    print(complete_name)
    with open(complete_name, 'r+') as js:
        return json.load(js)
    # TODO throw error?


def safe_open_w(path: str):
    # Open "path" for writing, creating any parent directories as needed.
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, 'wb+')


def save_avro(json_content: List[Dict[str, Any]], path: str) -> None:
    file_name = 'sales_' + os.path.basename(path)
    complete_name = os.path.join(path, file_name + ".avro")
    with safe_open_w(complete_name) as f:
        writer(f, parsed_schema, json_content, codec='deflate')
    print('done')
    pass