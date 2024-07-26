from utils.postgres_utils import get_data
import json


def get_source_metadata(db, data):
    x = get_data(db, data)
    return json.loads(x)
