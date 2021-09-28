import requests
from typing import List, Dict, Tuple, Union
from .utils.yamlconfig_parser import parse_config
from .utils.authentication import get_headers
from .models.sensei import Sensei
from .models.catalogservice import CatalogService
from .models.queryservice import QueryService
from .models.segmentationservice import SegmentationService
from .models.schemaregistry import SchemaRegistry
from .models.dataaccess import DataAccess
from .models.flowservice import FlowService
import json
import os
import warnings


class AEP:
    def __init__(self, config_path: str = None, config_data: str = None):
        """ Top class in which API object live. All requests are made through
        this class. Using the config a JWT token is obtained. Through this JWT
        token neccesary authentication headers are retrieved and saved in a
        requests.session object.

        :param config_path: Path to the config. Config contains information to establish authentication. 
        :type config_path: str
        """
        cfg = parse_config(path=config_path, data=config_data)

        self.session = requests.Session()
        headers = get_headers(cfg)
        self.session.headers.update(headers)
        self.known_endpoints = self._get_endpoints()
        # set up collections
        self.sensei = Sensei(self)
        self.catalog_service = CatalogService(self)
        self.query_service = QueryService(self)
        self.segmentation_service = SegmentationService(self)
        self.schema_registry = SchemaRegistry(self)
        self.data_access = DataAccess(self)
        self.flow_service = FlowService(self)

    @staticmethod
    def _get_endpoints() -> Dict:
        """Loads all the known endpoints.

        :return: Contains per endpoint the url and which extra headers are needed.
        Endpoints are grouped per collection. Available endpoints and their collections
        are 1-to-1 with the AEP api reference: www.adobe.io/apis/experienceplatform/home/api-reference.html
        :rtype: Dict
        """
        resource_path = os.path.join(os.path.split(__file__)[0], "resources")
        endpoint_path=os.path.join(resource_path,'known_endpoints.yaml')
        endpoint_param_path=os.path.join(resource_path,'endpoint_parameters.yaml')
        endpoint_params = parse_config(endpoint_param_path)
        return parse_config(endpoint_path, arg_replacements=endpoint_params)

    def _path_to_endpoint_and_headers(self, path: str) -> Tuple[str, Dict]:
        """ Helper function to enable dot indexing into the known endpoints.

        :param path: The path into known endpoints, where collection and endpoint are seperated by a '.'
        :type path: str
        :return: The endpoint url and the extra headers needed for that endpoint.
        :rtype: Tuple[str, Dict]
        """
        node = self.known_endpoints
        for step in path.split('.'):
            node = node[step]
        return node['endpoint_url'], node['extra_headers']

    def post(self, path: str, body: Union[Dict, List], params: Dict, url_suffix: str = '') -> Dict:
        """ Implements a post request to the endpoint specified via path.

        :param path: Path in known_endpoints, using dot notation.
        :type path: str
        :param body: The body of the request
        :type body: Union[Dict, List]
        :param params: Parameters in the request
        :type params: Dict
        :param url_suffix: What to append to the endpoint url, defaults to ''
        :type url_suffix: str, optional
        :return: The parsed json response of the request
        :rtype: Dict
        """
        return self.request('POST', path, body, params, url_suffix)
    
    def get(self, path: str, body: Union[Dict, List] = {}, params: Dict = {}, url_suffix: str ='') -> Dict:
        """ Implements a get request to the endpoint specified via path.

        :param path: Path in known_endpoints, using dot notation.
        :type path: str
        :param body: The body of the request
        :type body: Union[Dict, List]
        :param params: Parameters in the request
        :type params: Dict
        :param url_suffix: What to append to the endpoint url, defaults to ''
        :type url_suffix: str, optional
        :return: The parsed json response of the request
        :rtype: Dict
        """
        return self.request('GET', path, body, params, url_suffix)

    def update(self, path: str, body: Union[Dict, List], params: Dict, url_suffix: str = '') -> Dict:
        """ Implements a patch request to the endpoint specified via path.

        :param path: Path in known_endpoints, using dot notation.
        :type path: str
        :param body: The body of the request
        :type body: Union[Dict, List]
        :param params: Parameters in the request
        :type params: Dict
        :param url_suffix: What to append to the endpoint url, defaults to ''
        :type url_suffix: str, optional
        :return: The parsed json response of the request
        :rtype: Dict
        """
        return self.request('PATCH', path, body, params, url_suffix)

    def delete(self, path: str, body: Union[Dict, List], params: Dict, url_suffix: str = '') -> Dict:
        """ Implements a delete request to the endpoint specified via path.

        :param path: Path in known_endpoints, using dot notation.
        :type path: str
        :param body: The body of the request
        :type body: Union[Dict, List]
        :param params: Parameters in the request
        :type params: Dict
        :param url_suffix: What to append to the endpoint url, defaults to ''
        :type url_suffix: str, optional
        :return: The parsed json response of the request
        :rtype: Dict
        """
        return self.request('DELETE', path, body, params, url_suffix)

    def request(self, method: str, path: str, body: Union[Dict, List], params: Dict, url_suffix: str) -> Dict:
        """ Method used for sending requests. Uses a requests session for authentication.

        :param method: REST method, either POST, GET, DELETE, PATCH.
        :type method: str
        :param path: Path in known_endpoints, using dot notation.
        :type path: str
        :param body: The body of the request
        :type body: Union[Dict, List]
        :param params: Parameters in the request
        :type params: Dict
        :param url_suffix: What to append to the endpoint url
        :type url_suffix: str
        :return: The parsed json response of the request
        :rtype: Dict
        """
        base_url, extra_headers = self._path_to_endpoint_and_headers(path)
        url = base_url+url_suffix
        data = json.dumps(body)
        resp = self.session.request(method=method, url=url, data=data, params=params, headers=extra_headers)
        if resp.status_code not in [200, 201, 207, 202]:
            http_error_msg = u'%s HTTP request failed: %s for url: %s' % (resp.status_code, resp.text, url)
            raise requests.exceptions.HTTPError(http_error_msg)
        elif resp.status_code == 207:
            warnings.warn('Multistatus 207 response, check result text for individual status')
        elif resp.status_code == 202:
            warnings.warn('Multistatus 202 response, your request has been accepted but needs time to activate')
        return json.loads(resp.text)