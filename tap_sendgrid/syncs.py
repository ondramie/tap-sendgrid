# ./tap_sendgrid/syncs.py

import singer
from .streams import IDS
from .http import authed_get, end_of_records_check
from .utils import (
    trimmed_records,
    trim_members_all,
    get_results_from_payload,
    safe_update_dict,
    write_records,
    get_tap_stream_tuple,
    find_old_list_count,
    get_added_properties,
)

logger = singer.get_logger()
LIMIT = 500


class Syncer(object):

    def __init__(self, ctx):
        self.ctx = ctx

    def sync(self):
        logger.info("Starting sync")
        logger.debug(f"Selected catalog: {self.ctx.selected_catalog}")
        self.sync_alls()
        logger.info("Starting incrementals sync")
        self.sync_incrementals()
        logger.info("Finished sync")
        self.ctx.write_state()

    def sync_incrementals(self):
        logger.info("Starting sync_incrementals")
        logger.debug(
            f"Catalog entries: {[cat_entry.tap_stream_id for cat_entry in self.ctx.catalog.streams]}"
        )
        logger.debug(
            f"Selected entries: {[cat_entry.tap_stream_id for cat_entry in self.ctx.selected_catalog]}"
        )
        logger.debug(
            f"Selected catalog entries type: {type(self.ctx.selected_catalog)}"
        )

        for cat_entry in self.ctx.selected_catalog:
            logger.info("=" * 50)
            logger.info(f"Processing stream: {cat_entry.tap_stream_id}")
            logger.info(f"Stream selection state: {cat_entry.is_selected()}")
            logger.info(f"Stream metadata: {cat_entry.metadata}")
            stream = get_tap_stream_tuple(cat_entry.tap_stream_id)
            if stream:
                logger.info(f"Found stream tuple: {stream}")
                if stream.bookmark:
                    logger.info(f"Stream has bookmark: {stream.bookmark}")
                    syncer = getattr(self, f"sync_{stream.bookmark[1]}", None)
                    logger.info(f"Found syncer method: {syncer}")
                    if syncer:
                        syncer(stream, cat_entry.schema)
                        self.ctx.write_state()
            logger.info("=" * 50)

    def sync_timestamp(self, stream, schema):
        """
        Searches using created and updated ates as created doesn't also
        impact updated
        """
        start = self.ctx.update_start_date_bookmark(stream.bookmark)

        logger.info(f"Starting sync_timestamp for {stream.tap_stream_id}")

        for day in self.discrete_days_since_start(start):
            for search_term in ["created_at", "updated_at"]:
                params = {search_term: day.int_timestamp}
                logger.debug(f"Attempting sync for day {day.to_date_string()}")
                logger.debug(
                    f"Looking for contacts {search_term} on {day.to_date_string()}"
                )
                self.write_paged_records(stream, schema, params=params)
            self.ctx.set_bookmark(stream.bookmark, self.ctx.now_date_str())

    def write_paged_records(
        self, stream, schema, params=None, url_key=None, added_properties=None
    ):
        logger.info(f"Starting paged request to {stream.endpoint} with params {params}")
        for res in self.get_using_paged(stream, add_params=params, url_key=url_key):
            logger.debug(f"Got response with status {res.status_code}")
            results = res.json().get("recipients")
            logger.info(f"write_paged_records: schema: {schema}")
            if results:
                self.write_records(
                    schema, results, stream, added_properties=added_properties
                )

    @staticmethod
    def write_records(schema, results, stream, added_properties=None):
        logger.info(f"write_records: results: {results}")
        logger.info(f"write_records: schema {schema}")
        records = trimmed_records(schema, results, stream, added_properties)
        logger.info(
            f"Stream endpoint {stream.endpoint}, number of records: {len(records)}"
        )
        write_records(stream.tap_stream_id, records)

    def sync_end_time(self, stream, schema):
        """Sync from last timestamp to current time"""
        start = self.ctx.update_start_date_bookmark(stream.bookmark).int_timestamp
        end = self.ctx.now_seconds

        logger.info(f"Attempting to sync stream {stream.tap_stream_id}")
        logger.info(f"Start time: {start}, End time: {end}")
        logger.info(f"Stream endpoint: {stream.endpoint}")
        logger.info(f"sync_end_time: schema {schema}")

        # Prevent processing invalid time windows
        if start >= end:
            logger.warning(f"Start time {start} is >= end time {end}, skipping sync")
            return

        logger.debug(
            f"Starting to extract {stream.tap_stream_id} from {start} to {end}"
        )

        records_synced = False
        try:
            for results in self.get_using_offset(stream, start, end):
                if results:
                    records_synced = True
                    self.write_records(schema, results, stream)

            # Only update bookmark if we successfully processed the window
            if records_synced:
                self.ctx.set_bookmark(stream.bookmark, self.ctx.ts_to_dt(end))
            else:
                logger.info(f"No records found between {start} and {end}")

        except Exception as e:
            logger.error(f"Error syncing {stream.tap_stream_id}: {str(e)}")
            raise

    def sync_member_count(self, stream, schema):
        stream_type = trim_members_all(stream.tap_stream_id)

        for list in self.ctx.cache[stream_type]:
            old_list_count = find_old_list_count(
                list["id"], self.ctx.update_start_date_bookmark(stream.bookmark)
            )
            if list["member_count"] > old_list_count:
                logger.debug(
                    f"Starting to extract {stream.tap_stream_id} as list size now: {list['member_count']}, was: {old_list_count}"
                )

                self.get_and_write_members(list, stream, schema)
            else:
                logger.debug(
                    f"Not syncing {stream_type} {list['id']} as it is same size as last sync"
                )

    def sync_alls(self):
        for cat_entry in self.ctx.selected_catalog:
            logger.info(f"sync_alls: schema {cat_entry.schema}")
            stream = get_tap_stream_tuple(cat_entry.tap_stream_id)
            if not stream.bookmark:
                logger.info(f"Extracting all {stream.tap_stream_id}")

                results = self.get_alls(stream)
                self.write_records(cat_entry.schema, results, stream)
                self.ctx.update_cache(results, cat_entry.tap_stream_id)

    def get_and_write_members(self, list, stream, schema):
        logger.info(f"get_and_write_members: schema {schema}")

        added_properties = get_added_properties(stream, list["id"])

        if stream.tap_stream_id == IDS.GROUPS_MEMBERS:
            results = self.get_alls(stream, url_key=list["id"])
            self.write_records(
                schema, results, stream, added_properties=added_properties
            )

        else:
            self.write_paged_records(
                stream, schema, url_key=list["id"], added_properties=added_properties
            )

        self.ctx.save_member_count_state(list, stream)

    def get_alls(self, stream, url_key=None):
        logger.info(f"Endpoint {stream.endpoint}")
        endpoint = stream.endpoint.format(url_key) if url_key else stream.endpoint

        return get_results_from_payload(
            authed_get(stream.tap_stream_id, endpoint, self.ctx.config).json()
        )

    def get_using_paged(self, stream, add_params=None, url_key=None):
        page = 1
        page_size = 1000
        endpoint = stream.endpoint.format(url_key) if url_key else stream.endpoint

        while True:
            params = {"page": page, "page_size": page_size}
            safe_update_dict(params, add_params)
            r = authed_get(
                stream.tap_stream_id, endpoint, self.ctx.config, params=params
            )
            yield r
            if not end_of_records_check(r):
                page += 1
            else:
                break

    def get_using_offset(self, stream, start, end):
        offset = 0
        limit = LIMIT

        while True:
            params = dict(
                offset=offset,
                limit=limit,
                start_time=start,
                end_time=end,
            )
            logger.info(f"Making request to {stream.endpoint} with params {params}")

            try:
                r = authed_get(
                    stream.tap_stream_id,
                    stream.endpoint,
                    self.ctx.config,
                    params=params,
                )
                r.raise_for_status()  # Raise HTTP errors if any
                response_data = r.json()

                if not isinstance(response_data, list):  # Validate the response
                    logger.warning(f"Unexpected response format: {response_data}")
                    break

                logger.debug(
                    f"Response status: {r.status_code}, content: {response_data}"
                )
                yield response_data

                if len(response_data) < limit:  # No more data available
                    break

                offset += limit

            except Exception as e:
                logger.error(f"Error during request: {e}")
                break

    def discrete_days_since_start(self, start):
        """
        Return List timestamps each day since start
        """
        search_term = start.start_of("day")
        days_to_search = []
        while search_term <= self.ctx.now:
            days_to_search.append(search_term)
            search_term = search_term.add(days=1)

        return days_to_search
