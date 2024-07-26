from utils.postgres_utils import get_target_schema


def get_target_metadata(db, data):
    x = get_target_schema(db, data)
    d = {}
    for row in x:
        d[row[0]] = row[1]
    return d
