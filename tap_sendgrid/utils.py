# ./tap_sendgrid/utils.py

import singer

from .streams import PK_FIELDS, STREAMS, IDS

logger = singer.get_logger()


def get_results_from_payload(payload):
    """
    SG sometimes returns Lists or one keyed Dicts
    """
    if isinstance(payload, dict):
        return next(iter(payload.values()))

    else:
        return payload


def make_record_if_str(record, stream):
    """
    transform email string to dict for group suppression members
    """
    logger.info(f"Original record: {record}")
    if isinstance(record, str):
        record = {PK_FIELDS[stream.tap_stream_id][0]: record}

    return record


def send_selected_properties(schema, record, stream, added_properties=None):
    """
    Creates and returns new record with selected properties
    """
    r = make_record_if_str(record, stream)
    logger.info(f"Original record: {r}")
    logger.info(f"Raw schema input: {schema}")  # Add this line

    # Get schema properties
    schema_dict = schema.to_dict() if hasattr(schema, "to_dict") else schema
    logger.info(f"Schema dict: {schema_dict}")  # Add this line

    properties = schema_dict.get("properties", {})
    logger.info(f"Schema properties: {properties}")  # Add this line

    # Process record
    processed_record = {}
    for field_name, field_schema in properties.items():
        if field_name in r:
            value = r[field_name]
            # Allow null values if schema permits them
            if value is not None or (
                field_schema.get("type", []) and "null" in field_schema["type"]
            ):
                processed_record[field_name] = value
                logger.info(f"Added field {field_name} with value {value}")
            else:
                logger.info(f"Skipping null value for field {field_name}")
        else:
            logger.info(f"Field {field_name} not found in record")

    if not processed_record:
        logger.warning(f"No fields from record {r} matched schema {properties}")
        # Return original record as fallback when no fields match
        return r  # Add this line

    return processed_record


def trimmed_records(schema, data, stream, added_properties=None):
    """
    Takes raw data and details on what to sync and returns cleaned records
    with only selected fields
    """
    logger.info(f"Data received in trimmed_records: {data}")  # Debug log
    if not isinstance(data, list):
        data = [data]

    records = [
        send_selected_properties(schema, r, stream, added_properties)
        for r in data
        if r is not None  # Only process non-empty records
    ]

    logger.info(f"Records after processing: {records}")  # Debug log
    return records


def get_added_properties(stream, id):
    return {"%s_id" % stream.tap_stream_id.split("_")[0][:-1]: id}


def trim_members_all(tap_stream_id):
    """
    E.g. returns groups for groups_all
    """
    return tap_stream_id.split("_")[0]


def add_all(tap_stream_id):
    """
    Adds all to the generic term e.g. groups_all for groups
    """
    return tap_stream_id.split("-")[0] + "_all"


def find_old_list_count(list_id, all_lists_state):
    """
    Returns the last list size saved for the provided list
    :param list_id:
    :param all_lists_state:
    :return:
    """
    last_size = 0
    for x in all_lists_state:
        if x["id"] == list_id:
            last_size = x["member_count"]

    return last_size


def clean_for_cache(data, tap_stream_id):
    """
    For saving lists sizes to cache, clean to just ID and member count.
    Applicable to GROUPS, LISTS, and SEGMENTS
    """
    lookup_keys = {
        IDS.LISTS_ALL: "recipient_count",
        IDS.GROUPS_ALL: "unsubscribes",
        IDS.SEGMENTS_ALL: "recipient_count",
    }
    if tap_stream_id in lookup_keys:
        return [
            {"id": d["id"], "member_count": d[lookup_keys[tap_stream_id]]} for d in data
        ]
    else:
        return data


def safe_update_dict(obj1, obj2):
    if obj2:
        obj1.update(obj2)
    return obj1


def get_tap_stream_tuple(tap_stream_id):
    logger.info(f"Looking up stream for {tap_stream_id}")
    for s in STREAMS:
        if s.tap_stream_id == tap_stream_id:
            logger.info(f"Found stream: {s}")
            return s
        logger.warning(f"No stream found for {tap_stream_id}")


def write_metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(tap_stream_id, records):
    """Write records + count."""
    logger.info(f"Attempting to write {len(records)} records for {tap_stream_id}")
    try:
        singer.write_records(tap_stream_id, records)
        write_metrics(tap_stream_id, records)
        logger.info(f"Successfully wrote {len(records)} records for {tap_stream_id}")
    except Exception as e:
        logger.error(f"Error writing records: {e}")
