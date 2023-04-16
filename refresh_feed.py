# Refresh the feed file from the original source
# Only download the file if the source is newer than the local copy
# This code was adapted from https://forums.raspberrypi.com/viewtopic.php?t=152226#p998268

import email.utils
import os
import sys
import time
import requests

# First we construct a handful of functions - testing happens down at the end
def httpdate_to_ts(dt):
    time_tuple = email.utils.parsedate_tz(dt)
    return 0 if time_tuple is None else email.utils.mktime_tz(time_tuple)


def ts_to_httpdate(ts):
    return email.utils.formatdate(timeval=ts, localtime=False, usegmt=True)


def write_file_with_time(filename, content, timestamp):
    # put the content into the file
    with open(filename, 'wb') as fp:
        fp.write(content)

    # Then set the file's timestamps as requested
    os.utime(filename, times=(time.time(), timestamp))


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
    if mtime > int(last_mtime):
        updated = True
        r2 = requests.get(url)  # download the new file content
        if r2.status_code != requests.codes.ok:
            # http request failed
            print('HEY! get for {} returned {}'.format(url, r2.status_code),
                  file=sys.stderr)
            return False, last_mtime

        # write new content to local file
        write_file_with_time(local_file, r2.content, mtime)

    return updated, mtime


# v2: download remote file conditionally, with HTTP's If-Modified-Since header.
#     This requires the remote server to support both sending the Last-Modified
#     header and receiving the If-Modified-Since header.
#
def update_local_file_from_url_v2(last_mtime, local_file, url):

    # Get the remote file, but only if it has changed
    r = requests.get(url, headers={
                              'If-Modified-Since': ts_to_httpdate(last_mtime)
                          })

    updated, mtime = False, last_mtime

    if r.status_code == requests.codes.ok:
        # File is updated and we just downloaded the content
        updated = True

        # write new content to local file
        write_file_with_time(local_file, r.content, mtime)

        # Update our notion of the file's last modification time
        if 'Last-Modified' in r.headers:
            mtime = httpdate_to_ts(r.headers['Last-Modified'])
        else:
            print('HEY! no Last-Modified header for {}'.format(url),
                  file=sys.stderr)

    elif r.status_code == requests.codes.not_modified:
        # Successful call, but no updates to file
        print('As of {}, server says {} is the same'.format(time.ctime(), url))
    else:
        # http request failed
        print('HEY! get for {} returned {}'.format(url, r.status_code),
              file=sys.stderr)

    return updated, mtime
