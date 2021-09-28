import logging
import yaml
import requests
import warnings
from functools import wraps
import os
import copy
import time
from dictor import dictor
import json

# TODO: add flags in CLI to set logging level
def setup_logger(name, logging_level=logging.INFO):
    """
    :param name: name
    :return: logger
    """
    # create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging_level)

    # create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging_level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')

    # add formatter to console_handler
    console_handler.setFormatter(formatter)

    # add console_handler to logger
    logger.addHandler(console_handler)

    return logger


def http_request(method, url, headers, data=None, **kwargs):
    """
    Request util
    :param method: GET or POST or PUT
    :param url: url
    :param headers: headers
    :param data: optional data (needed for POST)
    :return: response text
    """
    response = requests.request(method, url, headers=headers, data=data, **kwargs)
    if response.status_code == 207:
        warnings.warn("HTTP status code 207 (multi-status), check response contents for individual status.")
    if response.status_code == 202:
        warnings.warn("HTTP status code 202 (accepted), processing might not be complete. Check response content.")
    if response.status_code not in [200, 201, 202, 207]:
        http_error_msg = u'%s HTTP request failed: %s for url: %s' % (response.status_code, response.text, url)
        raise requests.exceptions.HTTPError(http_error_msg)
    return response.text
	
# decocator to extend headers
def extend_header(header_extension):
    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if 'header' not in kwargs.keys():
                raise KeyError('Extend header decorator expects header in kwargs')
            kwargs['header'] = {**kwargs['header'],
                                **header_extension}
            return func(*args, **kwargs)
        return wrapper
    return inner

def get_aep_config(path=None):
    if path is None:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)),"aep_config.yaml")
    with open(path, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg

def poll_for_status(status_path, wait, failure, success, poll_url,
                   header, initial_wait, poll_wait, poll_tries, logger):
    logger.info("Waiting {} seconds before inital poll".format(initial_wait))
    time.sleep(initial_wait)
    for n_try in range(poll_tries):
        res_dict = json.loads(http_request("get", poll_url, header))
        status = dictor(res_dict, status_path)
        logger.info("status is {} for poll {}".format(status, n_try))
        if status == success:
            logger.info("Process has finished successfully")
            return res_dict
        elif status == wait:
            logger.info("Waiting {} seconds".format(poll_wait))
            time.sleep(poll_wait)
        elif status == failure:
            raise Exception("The process has failed (status {})".format(status))
        else:
            raise Exception("""Unknown status {}. Expecting status: "
        {} for success,
        {} for wait,
        {} for failure""".format(status, success, wait, failure))