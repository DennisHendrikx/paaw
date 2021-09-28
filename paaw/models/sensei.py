from __future__ import annotations
from paaw.utils.yamlconfig_parser import parse_config
from .abstractmodel import AEPCollection, AEPObject
import json
from requests_toolbelt.multipart.encoder import MultipartEncoder
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..aep import AEP


class Engine(AEPObject):
    name = 'engine'

    @classmethod
    def create_from_config(cls: Engine, collection: AEPCollection, config: Dict, _aep: AEP) -> Engine:
        """ Creates an engine from config. Overwrite inherited because post should send form-data

        :param cls: The engine class to instantiate
        :type cls: Engine
        :param collection: -
        :type collection: AEPCollection
        :param config: The configuration for the engine
        :type config: Dict
        :param _aep: Top class used to send requests
        :type _aep: AEP
        :return: An instance of engine that represents and engine artifact on AEP.
        :rtype: Engine
        """
        multipart_data = MultipartEncoder(
            fields={ 
                'engine': json.dumps(config)})
        url, extra_headers = _aep._path_to_endpoint_and_headers('sensei.engine')
        extra_headers['Content-Type'] = multipart_data.content_type
        response = _aep.session.request('POST', url=url, headers=extra_headers, data=multipart_data)
        definition = json.loads(response.text)
        return cls(definition, _aep)

    def __str__(self) -> str:
        """ string representation of engine

        :return: string representation
        :rtype: str
        """
        return 'engine with id {}'.format(self.id)


class MLInstance(AEPObject):
    name = 'mlinstance'
    def __str__(self)  -> str:
        """ string representationf mlinstance

        :return: string representation
        :rtype: str
        """
        return 'mlinstance with id {}'.format(self.id)


class Experiment(AEPObject):
    name = 'experiment'
    def start_experimentrun(self, config_path: str, arg_replacements: Dict) -> ExperimentRun:
        """ Returns an experimentrun for this experiment using a post request
        with config as body. 

        :param config_path: path to config yaml.
        :type config_path: str
        :param arg_replacements: values to replace
        :type arg_replacements: Dict
        :return: An instance of Experimentrun representing an experimentrun on AEP.
        :rtype: ExperimentRun
        """
        config = parse_config(config_path, arg_replacements=arg_replacements)
        return ExperimentRun.create_from_config(self, config, self._aep)

    def get_models(self) -> List[Model]:
        """ Returns a list of all models under this experiment.

        :raises Exception: Raised when no models are available 
        :return: List of model objects.
        :rtype: List[Model]
        """
        params = {'property': 'experimentId=={}'.format(self.id)}
        result = self._aep.get(path='sensei.model', body={}, params=params)
        if result["_page"]["count"] == 0:
            raise Exception("No model found for {}".format(self.id))
        else:
            return [Model(model_info, self._aep) for model_info in result['children']]

    def get_latest_model(self) -> Model:
        """ Gets the latest trained model for this experiment.

        :raises Exception: Raised when no model is available
        :return: An instance of model, which represents a model on AEP.
        :rtype: Model
        """
        params = {'property': 'experimentId=={}'.format(self.id),
                  'sortDescending': 0,
                  'sortField': 'created'}
        result = self._aep.get(path='sensei.model', body={}, params=params)
        if result["_page"]["count"] == 0:
            raise Exception("No model found for {}".format(self.id))
        else:
            model_info = result['children'][0]
            return Model(model_info, self._aep)

    def __str__(self)  -> str:
        """ string representation of experiment.

        :return: string representation
        :rtype: str
        """
        return 'experiment with id {}'.format(self.id)


class ExperimentRun(AEPObject):
    name = 'experimentrun'
    def __init__(self, id: str, definition: Dict, _aep: AEP, experiment_id: str):
        """ An experimentrun on AEP. Trains and/or scores a model.
        Overwrites inherited because experimentrun is always under a certain
        experiment.

        :param id: The id of this experimentrun
        :type id: str
        :param definition: Definition of this experimentrun.
        :type definition: Dict
        :param _aep: top class through which requests are made.
        :type _aep: AEP
        :param experiment_id: id of the experiment.
        :type experiment_id: str
        """
        super().__init__(id, definition, _aep)
        self.experiment_id = experiment_id
        base_url, _ = _aep._path_to_endpoint_and_headers('sensei.experiment')
        self.poll_url = base_url + '/' + self.experiment_id + '/runs/' + self.id + '/status'

    def poll_status(self) -> Dict:
        """ Retrieves the status of this experimentrun.

        :return: dictionary containing the status.
        :rtype: Dict
        """
        resp = self._aep.session.request(method='GET', url=self.poll_url)
        result = json.loads(resp.text)
        return result
        
    def get_model(self) -> Model:
        """ Retrieves a trained model for an experimentrun.

        :raises Exception: When no model is found.
        :raises Exception: when more than 1 model is found
        :return: The trained model for this experimentrun.
        :rtype: Model
        """
        params = {'property': 'experimentRunId=={}'.format(self.id)}
        result = self._aep.get(path='sensei.model', body={}, params=params)
        if result["_page"]["count"] == 0:
            raise Exception("No model found for {}".format(self.id))
        if result["_page"]["count"] > 1:
            raise Exception("More than 1 model found for {}".format(self.id))
        else:
            model_info = result['children'][0]
            return Model(model_info, self._aep)

    @classmethod
    def create_from_config(cls: ExperimentRun, collection: Experiment, config: Dict, _aep: AEP) -> ExperimentRun:
        """ Creates an experimentrun under a experiment. Overwrites inherited 
        because need the experiment as well. 

        :param cls: The class for experiment run to initialize.
        :type cls: ExperimentRun
        :param collection: The experiment under which this experimentrun is initialized.
        :type collection: Experiment
        :param config: config for this experimentrun.
        :type config: Dict
        :param _aep: Top class through which requests are made.
        :type _aep: AEP
        :return: Initialized experimentrun.
        :rtype: ExperimentRun
        """
        experiment_id = collection.id
        base_url, _ = _aep._path_to_endpoint_and_headers('sensei.experiment')
        url = base_url + '/' + experiment_id + '/runs'
        extra_headers = {
            'Content-Type': 'application/vnd.adobe.platform.sensei+json;profile=experimentRun.v1.json',
            'Accept': 'application/vnd.adobe.platform.sensei+json;profile=experimentRun.v1.json'}
        data = json.dumps(config)
        resp = _aep.session.request('POST', url=url, data=data, params={}, headers=extra_headers)
        result = json.loads(resp.text)
        return cls(result, _aep, experiment_id)


class Model(AEPObject):
    name = 'model'


class Sensei(AEPCollection):
    def __init__(self, _aep):
        """ Collection for ML Sensei endpoints.
        See: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#!acpdr/swagger-specs/sensei-ml-api.yaml

        :param _aep: Top class through which requests are made.
        :type _aep: [type]
        """
        super().__init__(_aep, 'sensei')

    def create_engine(self, config_path: str, arg_replacements: Dict) -> Engine:
        """ Creates an engine through a post request with body from config with 
        variable replacements.

        :param config_path: path to config yaml
        :type config_path: str
        :param arg_replacements: variables to replace in config.
        :type arg_replacements: Dict
        :return: An instance of engine, representing an engine on AEP.
        :rtype: Engine
        """
        return self._create_aepobject(Engine, config_path, arg_replacements)
    
    def get_engine(self, id: str) -> Engine:
        """ Retrieves an existing engine.

        :param id: id of existing engine.
        :type id: str
        :return: Instance of engine.
        :rtype: Engine
        """
        return self._get_aepobject(Engine, id)

    def create_mlinstance(self, config_path: str, arg_replacements: Dict) -> MLInstance:
        """ Creates an mlinstance through a post request with body from config
        with variable replacements.

        :param config_path: path to config yaml
        :type config_path: str
        :param arg_replacements: variables to replace in config.
        :type arg_replacements: Dict
        :return: And instance of MLInstance.
        :rtype: MLInstance
        """
        return self._create_aepobject(MLInstance, config_path, arg_replacements)
    
    def get_mlinstance(self, id: str) -> MLInstance:
        """ Retrieves an existing ml instance

        :param id: id of the existing ml instance.
        :type id: str
        :return: instance of mlinstance.
        :rtype: MLInstance
        """
        return self._get_aepobject(MLInstance, id)

    def create_experiment(self, config_path: str, arg_replacements: Dict) -> Experiment:
        """ Creates an experiment through a post request with body from config with
        variable replacements.

        :param config_path: path to config yaml
        :type config_path: str
        :param arg_replacements: variables to replace in config.
        :type arg_replacements: Dict
        :return: and instance of experiment.
        :rtype: Experiment
        """
        return self._create_aepobject(Experiment, config_path, arg_replacements)
    
    def get_experiment(self, id: str) -> Experiment:
        """ Retrieves existing experiment.

        :param id: id of the existing experiment.
        :type id: str
        :return: Instance of experiment.
        :rtype: Experiment
        """
        return self._get_aepobject(Experiment, id)
