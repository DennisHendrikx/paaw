import requests
from typing import List, Dict, Tuple, Union
from .utils.yamlconfig_parser import parse_config
from .utils.authentication import get_headers


class ACS:
    def __init__(self, config_path: str = None, config_data: str = None):
        """ Experimental class to also setup a session for requests to ACS.

        :param config_path: Path to the config. Config contains information to establish authentication. 
        :type config_path: str
        """
        cfg = parse_config(path=config_path, data=config_data)

        self.session = requests.Session()
        headers = get_headers(cfg)
        self.session.headers.update(headers)