from lesson_02.job2.dal import json_to_avro


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:
    print("\tI'm in get_sales(...) function!")
    data = json_to_avro.get_json(raw_dir)
    json_to_avro.save_avro(data, stg_dir)
