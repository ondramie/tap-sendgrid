#!/usr/bin/env python3

import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from singer.utils import parse_args

from . import streams
from .context import Context
from .http import authed_get
from .streams import Scopes
from .syncs import Syncer

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]


def _monkey_patch_is_selected(self):
    """Override is_selected to check metadata"""
    if hasattr(self, "metadata"):
        root_metadata = [m for m in self.metadata if not m.get("breadcrumb")]
        if root_metadata:
            return root_metadata[0].get("metadata", {}).get("selected", False)
    return False


CatalogEntry.is_selected = _monkey_patch_is_selected


def check_credentials_are_authorized(ctx):
    res = authed_get(Scopes.source, Scopes.endpoint, ctx.config)
    scopes = res.json().get("scopes", [])

    missing_auths = set(Scopes.scopes)
    for s in Scopes.scopes:
        if s in scopes:
            missing_auths.remove(s)

    if len(missing_auths):
        raise Exception(
            "Insufficient authorization, missing for {}".format(",".join(missing_auths))
        )


def discover(ctx):
    check_credentials_are_authorized(ctx)
    catalog = Catalog([])
    LOGGER.debug("stream.STREAMS: %s", streams.STREAMS)
    for stream in streams.STREAMS:
        LOGGER.debug(f"Processing stream: {stream.tap_stream_id}")
        schema = Schema.from_dict(
            streams.load_schema(stream.tap_stream_id), inclusion="available"
        )

        LOGGER.info(f"Processing stream: {stream.tap_stream_id}: {schema}")

        mdata = metadata.new()
        mdata = metadata.write(mdata, (), "selected", True)

        for prop in schema.properties:
            if prop in streams.PK_FIELDS[stream.tap_stream_id]:
                mdata = metadata.write(
                    mdata, ("properties", prop), "inclusion", "automatic"
                )
            else:
                mdata = metadata.write(
                    mdata, ("properties", prop), "inclusion", "available"
                )

        catalog_stream = CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=streams.PK_FIELDS[stream.tap_stream_id],
            schema=schema,
            metadata=metadata.to_list(mdata),
        )
        catalog.streams.append(catalog_stream)
    return catalog


def desired_fields(selected, stream_schema):
    """
    Returns fields that should be synced
    """
    all_fields = set()
    available = set()
    automatic = set()

    for field, field_schema in stream_schema.properties.items():
        all_fields.add(field)
        inclusion = field_schema.inclusion
        if inclusion == "automatic":
            automatic.add(field)
        elif inclusion == "available":
            available.add(field)
        else:
            raise Exception("Unknown inclusion " + inclusion)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            "Fields %s are required but were not selected. Adding them.",
            not_selected_but_automatic,
        )

    return selected.intersection(available).union(automatic)


def sync(ctx):
    check_credentials_are_authorized(ctx)

    LOGGER.info(f"ctx.selected_catalog: {ctx.selected_catalog}")

    for c in ctx.selected_catalog:
        LOGGER.info(f"c.schema: {c.schema}")
        selected_fields = set(
            [
                k
                for k, v in c.schema.properties.items()
                if v.selected
                or k == c.replication_key
                or v.inclusion in ("available", "automatic")
            ]
        )
        fields = desired_fields(selected_fields, c.schema)
        LOGGER.debug(f"c.schema.properties: {c.schema.properties}")
        LOGGER.debug(f"selected_fields: {selected_fields}")
        LOGGER.debug(f"fields: {fields}")

        schema = Schema(
            type="object",
            properties={prop: c.schema.properties[prop] for prop in fields},
        )

        LOGGER.info(f"Schema {schema}")
        c.schema = schema
        streams.write_schema(c.tap_stream_id, schema)

    syncer = Syncer(ctx)
    syncer.sync()


def main_impl():
    args = parse_args(REQUIRED_CONFIG_KEYS)
    LOGGER.debug(f"Starting tap with args: {args}")
    ctx = Context(args.config, args.state)
    LOGGER.debug(f"Context created: {ctx}")
    if args.discover:
        discover(ctx).dump()
    else:
        LOGGER.debug("Creating catalog from properties")
        LOGGER.debug(f"Properties structure: {args.properties}")

        ctx.catalog = (
            Catalog.from_dict(args.properties) if args.properties else discover(ctx)
        )
        LOGGER.info("Catalog created")

        LOGGER.debug(
            f"Loaded catalog streams: {[s.tap_stream_id for s in ctx.catalog.streams]}"
        )
        LOGGER.debug(
            f"Selected: {[s.tap_stream_id for s in ctx.catalog.streams if s.is_selected()]}"
        )

        sync(ctx)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise


if __name__ == "__main__":
    main()
