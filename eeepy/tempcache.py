import os
import uuid
import time
from eeepy import fileutil
import hashlib


class ChecksumError(IOError):
    """
    Raised when a file checksum fails.
    """
    pass


def _get_checksum(file_name):
    """
    Get the MD5 checksum of a file.

    :param file_name: File to check.

    :return: MD5 checksum.
    """

    # Code from Stack Overflow:
    # http://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file

    hash_md5 = hashlib.md5()

    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


class CacheEntry:
    """
    An entry for a cached file.
    """

    def __init__(self, file_path, temp_file, file_name, do_rm=True):
        """
        Create an entry for a cached file.

        :param file_path: Full path to the file.
        :param temp_file: Full path to the temporary file.
        :param file_name: Original file name `register` was called on.
        :param do_rm: Remove temporary file if `True`.
        """

        self.__file_path = str(file_path)
        self.__temp_file = str(temp_file)
        self.__file_name = str(file_name)
        self.__do_rm = bool(do_rm)

    def __str__(self):
        return '{} -> {} [rm={}]'.format(self.__temp_file, self.__file_path, self.__do_rm)

    def __repr__(self):
        return '[CacheEntry: temp_file={}, file_path={}]'.format(self.__temp_file, self.__file_path)

    def __getattr__(self, name):

        if name == 'file_path':
            return self.__file_path

        if name == 'temp_file':
            return self.__temp_file

        if name == 'file_name':
            return self.__file_name

        if name == 'do_rm':
            return self.__do_rm

        return self.__dict__[name]


class TempCache:
    """
    Manage files cached in a temporary location, and reliably write them back to a final storage location.
    """

    def __init__(self, temp_dir='temp', retry=2, retry_delay=4, copy_on_err=False, bandwidth=None, validate=True):
        """
        Create a new `TempCache` object.

        :param temp_dir: Directory where temporary files are located.
        :param retry: Number of times to retry copying a file after the first attempt.
        :param retry_delay: Sleep for this value raised to the power of the retry count between attempts. For example,
            the delay for the second retry attempt is `retry_delay ** 2`.
        :param copy_on_err: Attempt to copy files even if an error was encountered within a with/as block.
        :param bandwidth: Limit file copy rate if not None. May be a string ending with multiplier 'b', 'k', 'm', 'g',
            or 't'. If no muliplier is given or if an integer is used, 'k' is assumed.
        :param validate: Validate file transfers by computing an MD5 sum on the source and destination.
        """

        self.__temp_dir = str(temp_dir)

        self.__registered_file = dict()
        self.__copy_on_err = bool(copy_on_err)
        self.__retry = int(retry)
        self.__retry_delay = int(retry_delay)
        self.__bandwidth = bandwidth
        self.__validate = bool(validate)

        if self.__retry < 0:
            raise ValueError('Retry value must not be negative: %d' % self.__retry)

    def register(self, file_name, do_rm=True):
        """
        Register a file that should be cached.

        :param file_name: Name of the file that should be cached.
        :param do_rm: Remove temporary file when a with/as block is exited or `do_copy()` is called.

        :return: Absolute path to the temporary file.
        """

        # Get locations
        file_path = os.path.abspath(file_name)
        base_name = os.path.basename(file_path)

        # Create temporary file name
        temp_file = os.path.join(self.__temp_dir, '{}.{}.tmp'.format(base_name, uuid.uuid1().hex))

        if os.path.exists(temp_file):
            # Try two more file names

            file_count = 1

            while os.path.exists(temp_file) and file_count < 3:
                temp_file = '%s.%s.tmp' % (base_name, uuid.uuid1().hex)

            if os.path.exists(temp_file):
                raise IOError('Cannot resolve a temporary file name for {} after 3 attempts'.format(file_name))

        # Save
        self.__registered_file[temp_file] = CacheEntry(file_path, temp_file, file_name, do_rm)

        # Return temporary file
        return temp_file

    def list_tuples(self):
        """
        Iterate over entries as tuples where the temp file is the first tuple element, and the destination file
        is the second.

        :return: Entry iterator.
        """
        for temp_file in self.__registered_file:
            entry = self.__registered_file[temp_file]
            yield (entry.temp_file, entry.file_path)

    def __enter__(self):
        """
        Enter a with/as block.

        :return: self.
        """
        return self

    def __exit__(self, ex_type, ex_value, ex_tb):
        """
        Copy all files from temp.

        :param ex_type: Exception type or `None` if no exception was raised.
        :param ex_value: Exception value or `None' if no exception was raised.
        :param ex_tb: Traceback or `None` if no exception was raised.

        :return: False. Exceptions are never ignored.
        """

        # Copy all files
        if ex_type is None or self.__copy_on_err:
            self.do_copy()

        return False

    def do_copy(self):
        """
        Copy all registered files.
        """

        # Iterate through all files
        for temp_file in self.__registered_file:
            cache_entry = self.__registered_file[temp_file]

            # Copy
            self._copy_file(temp_file, cache_entry.file_path)

            # Remove
            if cache_entry.do_rm:
                self._rm_file(temp_file)

    def _copy_file(self, temp_file, dest_file):
        """
        Copy a temporary file to a destination file. Retry copy if it fails.

        :param temp_file:
        :param dest_file:
        :return:
        """

        # Check file
        if not os.path.isfile(temp_file):
            raise IOError('Cannot copy temp file: File does not exist: {}'.format(temp_file))

        # Initialize
        n_try = 0
        last_ex = None

        while n_try <= self.__retry:
            last_ex = None

            # Sleep between retries to allow a file system to recover
            if n_try > 0:
                time.sleep(int(self.__retry_delay ** n_try))

            # Copy
            try:
                fileutil.copyfile(temp_file, dest_file, bandwidth=self.__bandwidth)

            except Exception as ex:
                last_ex = ex

            # Validate
            self._validate_copy(temp_file, dest_file)

            # Increment the number of tries
            n_try += 1

        # Raise the last exception
        if last_ex is not None:

            # Remove destination path
            if os.path.exists(dest_file):
                for count in range(3):
                    try:
                        os.remove(dest_file)
                        break

                    except Exception:
                        time.sleep(5)  # Wait 5 seconds and try again

            # Rethrow
            raise last_ex

    def _rm_file(self, file):
        """
        Remove file.

        :param file: File to be removed.
        """

        # Check file
        if not os.path.isfile(file):
            raise IOError('Cannot remove file: File does not exist: {}'.format(file))

        # Initialize
        n_try = 0
        last_ex = None

        while n_try <= self.__retry:

            # Sleep between retries to allow a file system to recover
            if n_try > 0:
                time.sleep(int(self.__retry_delay ** n_try))

            # Remove file
            try:
                os.remove(file)
                break

            except EnvironmentError as ex:
                last_ex = ex

            # Increment the number of tries
            n_try += 1

        # Raise the last exception
        if last_ex is not None:
            raise last_ex

    def _validate_copy(self, temp_file, dest_file):
        """
        Validate the copy operation.
        """
        # TODO: Complete and document this method

        # Do not validate if disabled
        if not self.__validate:
            return

        # Get checksums and compare
        cksum_temp = _get_checksum(temp_file)
        cksum_dest = _get_checksum(dest_file)

        if cksum_temp != cksum_dest:
            raise ChecksumError('Temporary file and destination file checksum mismatch: Source=0x{}, Destination=0x{}'
                                .format(cksum_temp, cksum_dest))

        return

    # def __getattr__(self, name):
    #     """
    #     Get an attribute of this cache.
    #
    #     :param name: Name of the attribute.
    #
    #     :return: Attribute value.
    #     """
    #
    #     if name == 'temp_dir':
    #         return self.__temp_dir
    #
    #     if name == 'copy_on_err':
    #         return self.__copy_on_err
    #
    #     raise AttributeError('TempCache has no attribute: %s' % name)
    #
    # def __setattr__(self, name, value):
    #     """
    #     Set an attribute on this cache.
    #
    #     :param name: Name of the attribute.
    #     :param value: Value of the attribute.
    #     """
    #
    #     if name.startswith('__'):
    #         self.__dict__[name] = value
    #         return
    #
    #     if name == 'temp_dir':
    #         self.__temp_dir = str(value)
    #         return
    #
    #     if name == 'copy_on_err':
    #         self.__copy_on_err = bool(value)
    #         return
    #
    #     self.__dict__[name] = value


