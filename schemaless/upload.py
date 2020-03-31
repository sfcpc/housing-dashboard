# Lint as: python3
"""Upload artifacts to DataSF."""
import logging

import datasf


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


SCHEMALESS_VIEW_ID = "8cxy-fa9z"
UUID_VIEW_ID = "5udx-x8qv"
LIKELY_MATCHES_VIEW_ID = "bcw2-pzis"


def _replace(path, view_id):
    client = datasf.get_client()
    datasf.replace(client, view_id, path)


def upload_schemaless(path):
    logger.info("Uploading schemaless...")
    _replace(path, SCHEMALESS_VIEW_ID)


def append_schemaless_diff(path):
    """Appends contents of `path` to the schemaless dataset.

    NOTE: `path` is not the path to the full schemaless file; it is the path
    to the `diff_out_file` as created in `schemaless/create_schemaless.py`.
    All rows in `path` will be appended to the dataset on DataSF, even if
    those rows already exist."""
    client = datasf.get_client()
    datasf.upsert(client, SCHEMALESS_VIEW_ID, path)


def upload_uuid(path):
    logger.info("Uploading UUID map...")
    _replace(path, UUID_VIEW_ID)


def upload_likely_matches(path):
    logger.info("Uploading likely matches...")
    _replace(path, LIKELY_MATCHES_VIEW_ID)
