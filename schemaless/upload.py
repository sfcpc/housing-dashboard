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


def upload_uuid(path):
    logger.info("Uploading UUID map...")
    _replace(path, UUID_VIEW_ID)


def upload_likely_matches(path):
    logger.info("Uploading likely matches...")
    _replace(path, LIKELY_MATCHES_VIEW_ID)
