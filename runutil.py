"""
A collection of utilities for running commands and snakemake targets.
"""

import os
import subprocess
import sys

from eeepy import fileutil


def run_cmd(args, env=None):
    """
    Run a command with the proper environment set.

    :param args: A tuple of arguments starting with the command name.
    :param env: A dictionary of environment variables for the process to run in. If `None`, the current environment is
        used.

    :return: The return code of the command.
    """

    sys.stdout.flush()

    p = subprocess.Popen(args, env=env)

    p.wait()

    return p.returncode


class SnakeRunner:
    """
    Executes Snakemake targets.
    """

    def __init__(self, snakefile='snakefile', params=None, env=None, snake_cmd='snakemake', timestamp=True, rerun=True):
        """
        Initialize the snakemake runner.

        :param snakefile: Snakemake file. Assumed to be an absolute path or relative to the current working
            directory.
        :param env: A dictionary of environment variables the snakemake process will be executed in. If `None`, the
            current environment is used.
        :param snake_cmd: The snakemake command to run. May be a full path to the snakemake executable.
        :param timestamp: Log with timestamps.
        :param rerun: Rerun incomplete targets.
        """

        # Convert snakefile to a normalized absolute file name and raise IOError if it is not a regular file
        snakefile = fileutil.make_abs(snakefile)

        # Get environment snakemake will run in
        if env is None:
            env = os.environ.copy()

        if params is None:
            params = dict()

        # Assign fields
        self.snakefile = snakefile
        self.params = params
        self.env = env
        self.snake_cmd = snake_cmd
        self.timestamp = timestamp
        self.rerun = rerun

    def run(self, target, target_opts=None, params=None, dryrun=False):

        # Initialize run command
        snakemake_cmd = [
            self.snake_cmd
        ]

        # Set timestamp option
        if self.timestamp:
            snakemake_cmd.append('-T')

        # Set rerun option
        if self.rerun:
            snakemake_cmd.append('--rerun-incomplete')

        # Set dry-run option
        if dryrun:
            snakemake_cmd.append('--dryrun')

        # Set snakefile and target
        snakemake_cmd.extend((
            '--snakefile',
            self.snakefile,
            target
        ))

        # Set target options
        if target_opts is not None:
            snakemake_cmd.extend(target_opts)

        # Set parameters
        snakemake_cmd.extend([val for val in self._param_list_iter(params)])

        # Run snakemake command
        return run_cmd(snakemake_cmd, self.env)

    def _param_list_iter(self, params):
        """
        Return an iterator over parameters as "key=value" strings.

        :param params: Run target parameters. These will be added to the objects parameters.

        :return: An iterator over the parameters "key=value" strings.
        """

        # Get a dictionary of parameters
        run_params = self.params.copy()

        if params is not None:
            run_params.update(params)

        for key in params:
            yield print('{0}={1}'.format(key, params[key]))

    def _get_param_list(self, params, delim=' '):
        """
        Return a list of parameters as delimited list.

        :param params: Run target parameters. These will be added to the objects parameters.
        :param delim: Join parameter "key=value" strings on this string.

        :return: A joined string of parameter "key=value" pairs.
        """

        return delim.join(self._param_list_iter(params))
