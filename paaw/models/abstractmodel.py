from __future__ import annotations
from ..utils.yamlconfig_parser import parse_config
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..aep import AEP

class AEPObject():
    name = 'abstract'
    id_find_func = lambda self, definition: definition['id']
    def __init__(self, definition: Dict, _aep: AEP, id= None):
        """ Abstract object that describes an AEP artifact under a certain endpoint 
        as a Python class. Contains the unique id of the artifact on AEP and a 
        definition dictionary that describes the artifact details in AEP.

        :param id: The unique id of this artifact on AEP.
        :type id: str
        :param definition: A dictionary that would recreate this artifact in a post request.
         Normally also the result of making a get request for the id.
        :type definition: Dict
        :param _aep: The top class through which all requests are made.
        :type _aep: AEP
        """
        if id:
            self.id = str(id)
        else:
            self.id = str(self.id_find_func(definition))
        self.definition = definition
        self._aep = _aep
    @classmethod
    def create_from_config(cls: AEPObject, collection: AEPCollection, config: Dict, _aep: AEP) -> AEPObject:
        """ Creates an AEP artifact using a post request. Returns a class representing that
        AEP artifact.

        :param cls: The class representing the AEP artifact/endpoint.
        :type cls: AEPObject
        :param collection: The collection of endpoints this endpoint is part of.
        :type collection: AEPCollection
        :param config: The body for the post request to create the artifact.
        :type config: Dict
        :param _aep: The top class through which requests are made.
        :type _aep: AEP
        :return: An instance of the created class, which corresponds to some artifact on AEP. 
        :rtype: AEPObject
        """
        result = _aep.post(path='.'.join((collection.name, cls.name)), body=config, params={})
        return cls(result, _aep)
    

class AEPCollection:
    def __init__(self, _aep: AEP, name: str):
        """ A collection of endpoints

        :param _aep: Top class through which requests are made.
        :type _aep: AEP
        :param name: Name of this collection.
        :type name: str
        """
        self._aep = _aep
        self.name = name

    def _create_aepobject(self, cls: AEPObject, config_path: str, arg_replacements: Dict) -> AEPObject:
        """ Creates an AEP artifact through a post request. Body of post request is collected
        by parsing a yaml file at config_path using the neccecary value replacements 
        as specified by arg_replacements.

        :param cls: The class representing the AEP artifact/endpoint.
        :type cls: AEPObject
        :param config_path: Path to the config yaml that forms the body of the post request.
        :type config_path: str
        :param arg_replacements: Variable replacement when reading the config.
        :type arg_replacements: Dict
        :return: An instance of the created class, which corresponds to some artifact on AEP. 
        :rtype: AEPObject
        """
        config = parse_config(config_path, arg_replacements=arg_replacements)
        return cls.create_from_config(self, config, self._aep)
    
    def _get_aepobject(self, cls: AEPObject, id: str) -> AEPObject:
        """ Retrieves an existing AEP artifact through a get request.
        Which artifact to retrieve is specified by the id.

        :param cls: The class representing the AEP artifact/endpoint.
        :type cls: AEPObject
        :param id: the unique id for an exiting AEP artifact.
        :type id: str
        :return: An instance of the class, which corresponds to some artifact on AEP.
        :rtype: AEPObject
        """
        url_suffix = '/'+str(id)
        result = self._aep.get(path='.'.join((self.name,cls.name)), url_suffix=url_suffix)
        return cls(result, self._aep)

    @staticmethod
    def default_definition_extract_func(response):
        return response['items']
    
    def _get_aepobject_list(self, cls: AEPObject, definition_extract_func=None, get_params=None) -> List[AEPObject]:
        if definition_extract_func is None:
            definition_extract_func = self.default_definition_extract_func
        if get_params is None:
            get_params = {}
        result = self._aep.get(path='.'.join((self.name,cls.name)), params=get_params)
        definition_list = definition_extract_func(result)
        return [cls(item, self._aep) for item in definition_list]