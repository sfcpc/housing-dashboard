# Lint as: python3
"""Tools to upload files to DataSF."""
import logging
import os
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

from airflow.models.variable import Variable
from socrata.authorization import Authorization
from socrata import Socrata

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_client(user='', password=''):
    """Get a Socrata client for the given username and password."""
    if not user:
        user = os.getenv('DATASF_USER', Variable.get('DATASF_USER'))
        if not user:
            raise ValueError("No user for DataSF client")
    if not password:
        password = os.getenv('DATASF_PASS', Variable.get('DATASF_PASS'))
        if not password:
            raise ValueError("No password for DataSF client")
    auth = Authorization("data.sfgov.org", user, password)
    return Socrata(auth)


def _do_upload(revision, fp):
    pp = Path(fp)
    upload = revision.create_upload(pp.name)
    logger.info("Uploading %s", pp.name)
    with open(fp, 'rb') as inf:
        source = upload.csv(inf)
        output_schema = (
            source.get_latest_input_schema().get_latest_output_schema())
        print("[%s] Waiting on output schema transform..." % pp.name)
        output_schema.wait_for_finish()
    job = revision.apply(output_schema)
    job.wait_for_finish(
        progress=lambda job: print(
            '[%s] %s' % (pp.name, job.attributes['log'][0])),
        sleeptime=5,
    )


def upsert(client, view_id, fp, public=False):
    """Upsert a file to a DataSF dataset.

    Args:
        client: A Socrata client (from `get_client`)
        view_id: The "view id" of the Socrata dataset. It is the
            "5udx-x8qv" part of data.sfgov.org/dataset/foo/5udx-x8qv.
        fp: Path for a file to upsert.
        public: Bool. Is the dataset public?

    Note that upsert will *append* the given file to the end of the dataset,
    unless the file contains row_ids that already exist in the dataset, in
    which case those rows will be updated.
    """
    view = client.views.lookup(view_id)
    permission = 'public' if public else 'private'
    rev = view.revisions.create_update_revision(permission=permission)
    _do_upload(rev, fp)


def replace(client, view_id, fp, public=False):
    """Replace a dataset with the contents of a file.

    Args:
        client: A Socrata client (from `get_client`)
        view_id: The "view id" of the Socrata dataset. It is the
            "5udx-x8qv" part of data.sfgov.org/dataset/foo/5udx-x8qv.
        fp: Path for a file to upload.
        public: Bool. Is the dataset public?
    """
    view = client.views.lookup(view_id)
    permission = 'public' if public else 'private'
    rev = view.revisions.create_replace_revision(permission=permission)
    _do_upload(rev, fp)


def download(client, view_id, dest):
    """Download a dataset to `dest`."""
    logger.info("Fetching %s to %s" % (view_id, dest))
    auth = HTTPBasicAuth(client.auth.username, client.auth.password)
    uri = 'https://data.sfgov.org/api/views/%s/rows.csv?accessType=DOWNLOAD'
    with requests.get(uri % view_id, auth=auth, stream=True) as req:
        req.raise_for_status()
        with open(dest, 'wb') as outf:
            for chunk in req.iter_content(chunk_size=8192):
                if chunk:
                    outf.write(chunk)
    logger.info("done with %s" % dest)
    return dest
