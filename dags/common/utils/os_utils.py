import logging
import os


def make_paths(paths, exist_ok=True):
    """
    Create directories from paths recursively if they don't exists
    :param paths: str or list of complete paths to create
    :type paths: sequence
    :param exist_ok: Skip if the directory exists. Default is True
    :type exist_ok: bool
    """
    if isinstance(paths, str):
        paths = [paths]

    for path in paths:
        try:
            os.makedirs(path, exist_ok=exist_ok)
        except (OSError, FileExistsError) as err:
            logging.error('Failure during creation of the path: {} due to {}'.format(path, str(err)))
            raise err
