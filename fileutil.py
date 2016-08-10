"""
A collection of utilities for manipulating files.
"""

import errno
import os
import re
import time
import shutil
import stat


# Some code copied from Python 2.7 (commit 102514:65eb8d0ede75)

def make_abs_file(file_name, root_dir_name=None, check=True, allow_dir=True):
    """
    Make filename into a normalized absolute file name.

    :param file_name: Name of the file.
    :param root_dir_name: Directory of the file or `None` to use the current working directory.
    :param check: If `True`, raise `IOError` if the file does not exist or is not a regular file.
    :param allow_dir: `True` if `file_name` may be a directory.

    :return: Absolute file name.
    """

    if root_dir_name is None:
        root_dir_name = os.getcwd()

    file = os.path.normpath(os.path.join(root_dir_name, file_name))

    if check:
        if not os.path.exists(file):
            raise IOError(errno.ENOENT, 'File does not exist', file)

        if os.path.isdir(file):
            if not allow_dir:
                raise IOError(errno.EISDIR, 'Expected a file but found a directory', file)

        else:
            if not os.path.isfile(file):
                raise IOError(errno.ENOENT, 'Not a regular file', file)

    return file


def _path_match(file_name, pattern='.*', path_pattern='.*', path_filter=()):
    """
    Determine if a file or its path matches regex patterns or if it should be filtered.

    :param file_name: Full file path to check.
    :param pattern: Base file name must match this pattern.
    :param path_pattern: Full file path must match this pattern.
    :param path_filter: A list of filters that must not match the file path.

    :return: `True` if the file passes, and `False` if it does not.
    """

    # Match file pattern
    if not re.match(pattern, os.path.basename(file_name)):
        return False

    # Match path pattern
    if not re.match(path_pattern, file_name):
        return False

    # Apply path filters
    for path_filter_element in path_filter:
        if re.match(path_filter_element, file_name):
            return False

    # Accept file_name
    return True


def search_dir(search_dir_name, pattern='.*', path_pattern='.*', path_filter=(), follow_symlinks=True):
    """
    Recursively search a directory for files and return a list of all files.

    :param search_dir_name: Name of the search directory.
    :param pattern: The base filename must match this regular expression. This pattern is not implicitly anchored, and
        so this pattern may match any part of the file (use "^" and "$" to anchor).
    :param path_pattern: The absolute path name to the file must match this regular expression. All others are ignored.
        This pattern is not implicitly anchored, and so this pattern may match any part of the file (use "^" and "$" to
        anchor).
    :param path_filter: If a file path matches this regular expression, or any in this list of regular expressions
        (argument may be a string or a list of strings), then filter it out.
    :param follow_symlinks: Files and directories may be symbolic links if set.

    :return: A list of absolute file names.
    """

    all_files = []

    search_files = [make_abs_file(search_dir_name)]

    # Set path filter
    if isinstance(path_filter, str):
        path_filter = (path_filter, )

    # Check search directory
    if os.path.islink(search_files[0]) and not follow_symlinks:
        raise IOError(errno.ENOENT,
                      'Search directory is a link and symlinks are disabled: {}'.format(search_dir_name),
                      search_dir_name)

    # Iterate until search files are depleted
    while search_files:

        this_file = search_files.pop()

        if os.path.isfile(this_file):
            # Check filters and append
            if _path_match(this_file, pattern, path_pattern, path_filter):
                all_files.append(this_file)

        else:
            # Search directory
            for next_file in os.listdir(this_file):

                next_file = make_abs_file(next_file, this_file)

                # Check symlink
                if not follow_symlinks and os.path.islink(next_file):
                    continue

                # Check for file or directory
                if os.path.isfile(next_file) or os.path.isdir(next_file):
                    search_files.append(next_file)

    return all_files


def _parse_bandwidth(bw_spec):
    """
    Parse a bandwidth specification string. This value is a string that starts with an integer or floating-point
    number followed by an optional multiplier. The multipiler, B, K, M, G, and T, specifies that the bandwidth is
    given in bytes, kilobytes, megabytes, gigabytes, or terabytes, respectively. If a multiplier is not used, 'K'
    is assumed.

    :param bw_spec: Specification string.

    :return: Bandwidth as an integer. This integer is always a multiple of 1024 and never less than 1024.
    """

    # Parse size specification

    m = re.match("^([0-9]+(\.[0-9]+)?)([bkmgtBKMGT])?$", str(bw_spec))

    if m is None:
        raise ValueError('Unrecognized size specification: {}'.format(bw_spec))

    bw_value = float(m.group(1))
    multiplier = m.group(3)

    # Apply multiplier to the base value
    if multiplier is not None:
        bw_value *= {'B': 1, 'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3, 'T': 1024 ** 4}[multiplier.upper()]
    else:
        bw_value *= 1024

    # Convert to an integer that is a multiplier of 1024
    bw_value = int(bw_value / 1024) * 1024

    if bw_value == 0:
        bw_value = 1024

    # Return bandwidth value
    return bw_value


def _copyfileobj_bwlimited(fsrc, fdst, bandwidth, length=16*1024):
    """
    Copy data from file-like object `fsrc` to file-like object `fdst`.

    :param fsrc: Source.
    :param fdst: Destination.
    :param bandwidth: Bandwidth in bytes per second.
    :param length: Buffer length.
    """

    # Parse bandwidth specification
    bw_value = _parse_bandwidth(bandwidth)

    # Initialize time and size tracking
    bytes_copied = 0
    start_time = time.time()

    # Copy
    while 1:
        buf = fsrc.read(length)

        # Check for EOF
        if not buf:
            break

        # Update length
        bytes_copied += len(buf)

        # Copy
        fdst.write(buf)

        # Sleep to limit bandwidth
        wait_time = (bytes_copied + length / 2) / bw_value - (time.time() - start_time)

        if wait_time > 0.01:
            time.sleep(wait_time)


def copyfileobj(fsrc, fdst, length=16*1024, bandwidth=None):
    """
    Copy data from file-like object `fsrc` to file-like object `fdst`.

    :param fsrc: Source.
    :param fdst: Destination.
    :param length: Buffer length.
    :param bandwidth: Bandwidth in bytes per second.
    """

    # Base code from Python 2.7 (commit 102514:65eb8d0ede75)

    if bandwidth is None:
        while 1:
            buf = fsrc.read(length)

            if not buf:
                break

            fdst.write(buf)

    else:
        _copyfileobj_bwlimited(fsrc, fdst, bandwidth, length)


def _samefile(src, dst):
    """
    Check `src` and `dst` to see if they point to the same file.

    :param src: Source file.
    :param dst: Destination file.

    :return: `True` if files are the same.
    """

    # Base code from Python 2.7 (commit 102514:65eb8d0ede75)

    # Macintosh, Unix.
    if hasattr(os.path, 'samefile'):
        try:
            return os.path.samefile(src, dst)
        except OSError:
            return False

    # All other platforms: check for same pathname.
    return (os.path.normcase(os.path.abspath(src)) ==
            os.path.normcase(os.path.abspath(dst)))


def copyfile(src, dst, bandwidth=None):
    """
    Copy data from src to dst.

    :param src: Source file.
    :param dst: Destination file.
    :param bandwidth: Limit copy rate to bandwidth KB/s if not 'None'.
    """

    # Base code from Python 2.7 (commit 102514:65eb8d0ede75)

    if _samefile(src, dst):
        raise shutil.Error("`%s` and `%s` are the same file" % (src, dst))

    for fn in [src, dst]:
        try:
            st = os.stat(fn)

        except OSError:
            # File most likely does not exist
            pass

        else:
            # XXX What about other special files? (sockets, devices...)
            if stat.S_ISFIFO(st.st_mode):
                raise shutil.SpecialFileError("`%s` is a named pipe" % fn)

    with open(src, 'rb') as fsrc:
        with open(dst, 'wb') as fdst:
            copyfileobj(fsrc, fdst, bandwidth=bandwidth)


def copymode(src, dst):
    """
    Copy mode bits from src to dst

    :param src: Copy mode bits from.
    :param dst: Copy mode bits to.
    """

    # Base code from Python 2.7 (commit 102514:65eb8d0ede75)

    if hasattr(os, 'chmod'):
        st = os.stat(src)
        mode = stat.S_IMODE(st.st_mode)
        os.chmod(dst, mode)


def copy(src, dst, bandwidth=None):
    """
    Copy data and mode bits ("cp src dst"). The destination may be a directory.

    The `bandwidth` argument is a string that starts with an integer or floating-point number followed by an optional
    multiplier. The multipiler, B, K, M, G, and T, specifies that the bandwidth is given in bytes, kilobytes, megabytes,
    gigabytes, or terabytes, respectively. If a multiplier is not used, "K" is assumed. If the specifier is an integer,
    then "K" is also assumed.

    :param src: Source file
    :param dst: Destination file or directory.
    :param bandwidth: Limit bandwidth or `None` to copy at full speed.
    """

    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))

    copyfile(src, dst, bandwidth)
    copymode(src, dst)
