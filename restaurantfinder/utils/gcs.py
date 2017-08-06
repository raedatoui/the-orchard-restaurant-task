from __future__ import absolute_import

import logging
from hashlib import md5

from google.appengine.api import app_identity

import cloudstorage as gcs
import cloudstorage.cloudstorage_api as gcs


STORAGE_LOCAL_BUCKET_PREFIX = '/_ah/gcs'
STORAGE_LIFE_BUCKET_PREFIX = 'https://storage.googleapis.com'


class DatastoreError(Exception):
    pass


class MD5ChecksumException(Exception):
    pass


def abs_filename(filename):
    bucket_name = app_identity.get_default_gcs_bucket_name()

    if bucket_name is None:
        raise DatastoreError(
            "Error", "Default GCS bucket is not set for this environment"
        )

    return '/{0}/{1}'.format(bucket_name, filename)


def _stat_file(file_path):
    """

    :param filename: full path
    :return:
    """
    st = gcs.stat(file_path)
    return {
        'filename': st.filename,
        'is_dir': st.is_dir,
        'st_size': st.st_size,
        'st_ctime': st.st_ctime,
        'etag': st.etag,
        'content_type': st.content_type,
        'metadata': st.metadata,
    }


def write(filename, content_type, data, public=True, verify_checksum=False, overwrite=False):
    """
    :param filename:
    :param mimetype:
    :param data:
    :param public:
    :return:
    """
    acl_opts = 'public-read'
    if public is False:
        acl_opts = 'private'

    file_path = abs_filename(filename)

    if gcs._file_exists(file_path) and not overwrite:
        return file_path, _stat_file(file_path)

    try:
        gcs_file = gcs.open(
            file_path,
            'w',
            content_type=content_type,
            options={'x-goog-acl': acl_opts}
        )
        gcs_file.write(data)
        gcs_file.close()

    except Exception as e:
        msg = "Fail to write to {0}".format(e)
        raise DatastoreError("Error", msg)

    stat = _stat_file(file_path)

    if verify_checksum:
        md5_checksum = md5(data).hexdigest()
        if stat['etag'] != md5_checksum:
            err = "Md5 Checksum error: MD5 1: {}. MD5 2: {}".format(stat['etag'],
                                                                    md5_checksum)
            raise MD5ChecksumException(err)

    return file_path, stat


def get_serving_url_from_filepath(file_path):
    return STORAGE_LIFE_BUCKET_PREFIX + file_path


def get_serving_url(file_stat):
    return get_serving_url_from_filepath(file_stat['filename'])


def read(filename):
    """
    includes the bucket. comes from a stored render
    :param filename:
    :return:
    """
    gcs_file = gcs.open(filename)
    content = gcs_file.read()
    gcs_file.close()
    return content


def delete(filename):
    """
    includes the bucket. comes from a stored render
    :param filename:
    :return:
    """
    try:
        gcs.delete(filename)
    except gcs.NotFoundError:
        logging.warning('File Deletion Failed {0}'.format(filename))

