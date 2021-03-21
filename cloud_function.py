import csv
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List

from google.cloud import storage
from sqlalchemy.sql import text
from sqlalchemy.engine import Connection, create_engine

logger = logging.getLogger(__name__)

# Create a database engine
engine = create_engine(os.getenv("DATABASE_URL"))


@dataclass
class Record:
    buyer: str
    item_id: int
    quantity: int
    price: int
    purchase_date: datetime


def is_event_valid(event: dict) -> bool:
    """
    Validate that the event has all the necessary attributes required for the
    execution.
    """

    attributes = event.keys()
    required_parameters = ["bucket", "contentType", "name", "size"]

    return all(parameter in attributes for parameter in required_parameters)


def is_object_valid(event: dict) -> bool:
    """
    Validate that the finalized/created object is a CSV file and its size is
    greater than zero.
    """

    has_content = int(event["size"]) > 0
    is_csv = event["contentType"] == "text/csv"

    return has_content and is_csv


def get_content(bucket: storage.Bucket, file_path: str) -> str:
    """
    Get the blob from the bucket and return its content as a string.
    """

    blob = bucket.get_blob(file_path)
    return blob.download_as_string().decode("utf-8")


def anonymize_pii(row: List[str]) -> Record:
    """
    Unpack and anonymize data.
    """

    email, item_id, quantity, price, purchase_date = row

    # Anonymize email address
    hashed_email = hashlib.sha1(email.encode()).hexdigest()

    return Record(
        buyer=hashed_email,
        item_id=int(item_id),
        quantity=int(quantity),
        price=int(price),
        purchase_date=purchase_date,
    )


def write_to_db(conn: Connection, record: Record):
    """
    Write the records into the database.
    """

    query = """
    INSERT INTO purchases(buyer, item_id, quantity, price, purchase_date)
    VALUES(:buyer, :item_id, :quantity, :price, to_timestamp(:purchase_date, 'yyyy-MM-ddTHH:mm:ss'));
    """

    try:
        conn.execute(text(query), **record.__dict__)
    except Exception as exc:
        # If an error occures, log the exception and continue
        logger.exception("cannot write record", exc_info=exc)


def entrypoint(event: dict, context):
    """
    Triggered by a creation on a Cloud Storage bucket.
    """

    # Check if the event has all the necessary parameters. In case any of the
    # required parameters are missing, return early not to waste execution time.
    if not is_event_valid(event):
        logger.error("invalid event: %s", json.dumps(event))
        return

    file_path = event["name"]

    # Check if the created object is valid or not. In case the object is invalid
    # return early not to waste execution time.
    if not is_object_valid(event):
        logger.warning("invalid object: %s", file_path)
        return

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(event["bucket"])

    data = get_content(bucket, file_path)
    reader = csv.reader(data.splitlines())

    # Anonymize PII and filter out invalid records
    records: List[Record] = filter(lambda r: r, [anonymize_pii(row) for row in reader])

    # Write the anonymized data to database
    with engine.connect() as conn:
        for record in records:
            write_to_db(conn, record)
