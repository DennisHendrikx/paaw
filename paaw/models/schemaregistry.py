from __future__ import annotations
import urllib.parse
from paaw.utils.yamlconfig_parser import parse_config
from .abstractmodel import AEPCollection, AEPObject
from typing import List, Dict, Tuple, TYPE_CHECKING
import warnings
if TYPE_CHECKING:
    from ..aep import AEP


class Schema(AEPObject):
    name = 'schema'
    id_find_func = lambda self, definition: definition['$id']

    def refresh_definition(self):
        """ Get the new definition of the schema through a get request.
        Note that we need to url encode the id before the get request.
        """
        encoded_id = urllib.parse.quote_plus(self.id)
        result = self._aep.get(path='schemaregistry.schema',
                              body={},
                              params={},
                              url_suffix='/'+encoded_id)
        self.definition = result

    def patch_obj(self, op: str, path: str, value: object):
        """ Updates the schema via a patch request.

        :param op: Which patch operation to perform.
        :type op: str
        :param path: The path to the value to patch.
        :type path: str
        :param value: Then new value.
        :type value: object
        """
        body = [{
                "op": op,
                "path": path,
                "value": value
            }]
        stripped_id = self.id.split('/')[-1]
        self._aep.update(path='schemaregistry.schema', 
                         body=body, 
                         params={}, 
                         url_suffix='/_kpnbv.schemas.'+stripped_id)
        self.refresh_definition()

    def enable_for_profile(self):
        warnings.warn("Enabling for profile is irreversable")
        self.patch_obj(op='add', path='/meta:immutableTags', value=["union"])


class FieldGroup(AEPObject):
    name = 'fieldgroup'
    id_find_func = lambda self, definition: definition['$id']


class Descriptor(AEPObject):
    name = 'descriptor'
    id_find_func = lambda self, definition: definition['@id']


class SchemaRegistry(AEPCollection):
    def __init__(self, _aep: AEP):
        """ A collection for schema registry,
        see: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#/Schemas.

        :param _aep:The top class through which all requests are made.
        :type _aep: AEP
        """
        super().__init__(_aep, 'schemaregistry')
    
    def create_fieldgroup(self, config_path: str, arg_replacements: Dict) -> FieldGroup:
        """ Creates a new fieldgroup object that corresponds to a fieldgroup on AEP.
        One or more fieldgroups together create a schema.

        :param config_path: Path to the config yaml that describes the fieldgroup.
        :type config_path: str
        :param arg_replacements: Replacements in the config yaml.
        :type arg_replacements: Dict
        :return: The created fieldgroup.
        :rtype: FieldGroup
        """
        return self._create_aepobject(FieldGroup, config_path, arg_replacements)

    def get_fieldgroup(self, id: str) -> FieldGroup:
        """ Gets an existing fieldgroup by returning a fieldgroup object that 
        corresponds to a fieldgroup on AEP. 

        :param id: The id of the fieldgroup. Note that this looks like an url.
        :type id: str
        :return: fieldgroup object representing a fieldgroup on AEP.
        :rtype: FieldGroup
        """
        encoded_id = urllib.parse.quote_plus(id)
        return self._get_aepobject(FieldGroup, encoded_id)
    
    def create_schema(self, config_path: str, arg_replacements: Dict) -> Schema:
        """ Creates a new schema object that corresponds to a schema on AEP.
        Schema's describe the structure for a dataset.

        :param config_path: Path to the config yaml that describes the schema.
        :type config_path: str
        :param arg_replacements: Replacements in the config yaml.
        :type arg_replacements: Dict
        :return: The created schema.
        :rtype: Schema
        """
        return self._create_aepobject(Schema, config_path, arg_replacements)
    
    def get_schema(self, id: str) -> Schema:
        """ Gets an existing schema by returning a schema object that corresponds
        to a schema on AEP.

        :param id: The id of the schema. Note that this looks like an url.
        :type id: str
        :return: Schema object representing a schema on AEP.
        :rtype: Schema
        """
        encoded_id = urllib.parse.quote_plus(id)
        print(encoded_id)
        return self._get_aepobject(Schema, encoded_id)
    
    def create_descriptor(self, config_path: str, arg_replacements: Dict) -> Descriptor:
        """ Creates a new descriptor object that corresponds to a descriptor on AEP.
        Descriptors describe which fields in a schema are identities.

        :param config_path: Path to the config yaml that describes the descriptor.
        :type config_path: str
        :param arg_replacements: Replacements in the config yaml.
        :type arg_replacements: Dict
        :return: The created descriptor.
        :rtype: Descriptor
        """
        return self._create_aepobject(Descriptor, config_path, arg_replacements)
    
    def get_descriptor(self, id: str) -> Descriptor:
        """ Gets an existing descriptor by returning a descriptor object that
        corresponds to a schema on AEP.

        :param id: The id of the descriptor.
        :type id: str
        :return: Descriptor object representing a descriptor on AEP.
        :rtype: Descriptor
        """
        return self._get_aepobject(Descriptor, id)
