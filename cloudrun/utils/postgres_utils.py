import sqlalchemy


def get_data(db, data):
    get_col_id = "MetadataJson"
    table_id = "Metadata_Details"
    where_col_id = "MetadataDetailId"
    where_data = data['MetadataDetailId']
    query = sqlalchemy.text(
        f"""
        select "{get_col_id}" from snconfig."{table_id}" where "{where_col_id}" = {where_data} limit 1"""
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            results = conn.execute(query)
            for row in results:
                return row[0]
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.
        # [START_EXCLUDE]
        print(f"Exception raised in get data {e}")
        return f"Failed to get metadata"


def get_target_schema(db, data):
    target_table = data['TargetDetails']['target_table']
    query = f"""                              
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{target_table}';
        """
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            results = conn.execute(sqlalchemy.text(query)).fetchall()
            return results
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.
        # [START_EXCLUDE]
        print("Exception raised in get target schema")


def run_insert_query(db, query):
    try:
        with db.connect() as conn:
            conn.execute(sqlalchemy.text(query))
            conn.commit()
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.
        # [START_EXCLUDE]
        print(f"Exception raised in Run query for {query} : {e}")


def get_results_query(db, query):
    try:
        with db.connect() as conn:
            results = conn.execute(sqlalchemy.text(query)).fetchall()
            return results
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.
        # [START_EXCLUDE]
        print(f"Exception raised in Run query for {query} : {e}")

    return "None"
