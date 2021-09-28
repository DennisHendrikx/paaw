from __future__ import annotations
from .abstractmodel import AEPCollection, AEPObject
from .dataaccess import DataSetFile
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..aep import AEP
import io
import pandas as pd
import warnings

class Dataset(AEPObject):
    name = 'dataset'
    id_find_func = lambda self, definition: list(definition.keys())[0]

    def delete(self):
        """ Deletes the dataset. Clears this class of its id and definition
        to signify underlying artifact on AEP is deleted.
        """
        result = self._aep.delete(path='catalogservice.dataset',
                                  body={}, 
                                  params={}, 
                                  url_suffix='/'+self.id)
        self.definition=None
        self.id=None

    def get_batches(self, status:str = 'success'):
        if status is None:
            params = {'dataSet': self.id}
        else:
            params= {'dataSet': self.id, 'status': status}
        result = self._aep.get(
            path='catalogservice.batch',
            body={},
            params=params)
        batches = [Batch(definition=batch_def, _aep=self._aep, id=batch_id) for batch_id, batch_def in result.items()]
        return batches

class Batch(AEPObject):
    name = 'batch'

    def get_datasetfiles(self):
        """ Returns a list of the underlying datasetfiles

        :return: List of the datasetfiles in this batch.
        :rtype: List[DataSetFile]
        """
        result = self._aep.get(
            path='dataaccess.dataaccess',
            body={},
            params={},
            url_suffix='/'+self.id+'/files'
        )
        if result['_page']['count'] == 0:
            warnings.warn("This batch has no underlying datasetfiles")
            files = []
        else:
            files = [DataSetFile(definition=datasetfile_def, _aep=self._aep) 
                    for datasetfile_def in result["data"]]
        return files


class CatalogService(AEPCollection):
    def __init__(self, _aep: AEP):
        """ A collection for endpoints under CatalogService.
        See: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#!acpdr/swagger-specs/catalog.yaml

        :param _aep: the top class though which requests are made.
        :type _aep: AEP
        """
        super().__init__(_aep, 'catalogservice')
    
    def create_dataset(self, config_path: str, arg_replacements: Dict) -> Dataset:
        """ Creates a dataset for though post for body defined by yaml file 
        at config_path. Parses this config using replacements in arg_replacements.

        :param config_path: Path to the config yaml
        :type config_path: str
        :param arg_replacements: Argument replacments in config yaml.
        :type arg_replacements: Dict
        :return: A dataset instance corresponding to a dataset on AEP. 
        :rtype: Dataset
        """
        return self._create_aepobject(Dataset, config_path, arg_replacements)

    def get_dataset(self, id: str) -> Dataset:
        """ Retrieves an existing dataset. Overwrites inherited method 
        because id for dataset is nested in an url.

        :param id: Unique id of the dataset to be retrieved.
        :type id: str
        :return: A dataset instance corresponding to a dataset on AEP.
        :rtype: Dataset
        """
        return self._get_aepobject(Dataset, id)