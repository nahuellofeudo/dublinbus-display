# Refresh the feed file from the original source
# Only download the file if the source is newer than the local copy
# This code was adapted from https://forums.raspberrypi.com/viewtopic.php?t=152226#p998268

import email.utils
import os
import sys
import time
import requests
import urllib3

# First we construct a handful of functions - testing happens down at the end
def httpdate_to_ts(dt):
    time_tuple = email.utils.parsedate_tz(dt)
    return 0 if time_tuple is None else email.utils.mktime_tz(time_tuple)


def ts_to_httpdate(ts):
    return email.utils.formatdate(timeval=ts, localtime=False, usegmt=True)

# v1: download remote file if HTTP's Last-Modified header indicates that
#     the file has been updated. This requires the remote server to support
#     sending the Last-Modified header.
#
def update_local_file_from_url_v1(last_mtime, local_file, url):

    # Check the status of the remote file without downloading it
    r1 = requests.head(url)
    if r1.status_code != requests.codes.ok:
        # http request failed
        print('HEY! get for {} returned {}'.format(url, r1.status_code),
              file=sys.stderr)
        return False, last_mtime

    # Get the modification time for the file, if possible
    if 'Last-Modified' in r1.headers:
        mtime = httpdate_to_ts(r1.headers['Last-Modified'])
    else:
        print('HEY! no Last-Modified header for {}'.format(url),
              file=sys.stderr)
        return False, last_mtime

    # If file is newer than last one we saw, get it
    updated = False
    print('Comparing feed mtimes: feed: {} vs remote {}'.format(str(last_mtime), str(mtime)), file=sys.stderr)
    if not last_mtime or mtime > int(last_mtime):
        print('Refreshing feed..', file=sys.stderr)
        updated = True
        # download the new file content
        conn = urllib3.connection_from_url(url)
        r2 = conn.request(method="GET", url=url, preload_content=False) 
        if r2.status != 200:
            # http request failed
            print('HEY! get for {} returned {}'.format(url, r2.status_code),
                file=sys.stderr)
            try:
                r2.release_conn()
            except Exception as e:
                print('Could not release connection to {}: {}'.format(url, str(e)))
            return False, last_mtime

        with open(local_file,'bw') as f:
            for chunk in r2.stream(amt=65536, decode_content=True):
                f.write(chunk)

        r2.release_conn()
        # Change the mtime of the file
        os.utime(local_file, (mtime, mtime))

        # write new content to local file
        print('Downloaded {}.'.format(local_file), file=sys.stderr)
    else:   
        print('No need to refresh feed.', file=sys.stderr)

    return updated, mtime