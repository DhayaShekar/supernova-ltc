import signal
import sys
from types import FrameType
import json
from flask import Flask, request
from utils.connect_connector import connect_with_connector
from utils.metadata_sql import upload_metadata, update_metadata
from utils.source_metadata_handler import get_source_metadata
from utils.target_metadata_handler import get_target_metadata
from utils.validation_engine import validate
from utils.logging import logger

app = Flask(__name__)
db = connect_with_connector()


@app.route("/check_metadata", methods=['POST'])
def check_metadata_app() -> str:
    # Use basic logging with custom fields
    logger.info(logField="custom-entry", arbitraryField="custom-entry")
    # https://cloud.google.com/run/docs/logging#correlate-logs
    logger.info("Child logger with trace Id.")

    data = request.get_json()
    # ToDo Send The configuration data to Config Parser --> Dictionary --> Divya

    # Todo get the source metadata based on dictionary --> Dhaya
    source_metadata = get_source_metadata(db, data)

    # Todo Get the target metadata based on dictionary --> Dhaya
    target_metadata = get_target_metadata(db, data)

    # Todo Compare the metadata based on source metadata and Target Metadata --> Dhaya
    validate_tag = validate(db, source_metadata, target_metadata)



    return f"Metadata source : {source_metadata} , Target metadata : {target_metadata} , Validation is {validate_tag}"


@app.route("/upload_metadata", methods=['POST'])
def upload_metadata_app() -> str:
    # Use basic logging with custom fields
    data = request.get_json()
    logger.info(logField="custom-entry", arbitraryField="custom-entry")
    upload_metadata(db, data)
    # https://cloud.google.com/run/docs/logging#correlate-logs
    logger.info("Child logger with trace Id.")

    return f"Metadata uploaded in DB"


@app.route("/update_metadata", methods=['POST'])
def update_metadata_app() -> str:
    # Use basic logging with custom fields
    logger.info(logField="custom-entry", arbitraryField="custom-entry")
    data = request.get_json()
    update_metadata(db, data)
    # https://cloud.google.com/run/docs/logging#correlate-logs
    logger.info("Child logger with trace Id.")

    return f"Metadata updated in DB"


def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    flush()

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    app.run(host="localhost", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)
