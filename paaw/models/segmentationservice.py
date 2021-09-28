from __future__ import annotations
from .abstractmodel import AEPCollection, AEPObject
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..aep import AEP
    

class SegmentJob(AEPObject):
    name='segmentjob'
    def poll_status(self) -> Dict:
        """ Retrieves the status of the segmentjob with a get request.

        :return: [description]
        :rtype: Dict
        """
        return self._aep.get(path='segmentationservice.segmentjob',
                             body={},
                             params={},
                             url_suffix='/'+self.id)


class SegmentationService(AEPCollection):
    def __init__(self, _aep: AEP):
        """ A collection of endpoints for segmentationservice.
        See: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#!acpdr/swagger-specs/segmentation.yaml

        :param _aep: Top class through which requests are made.
        :type _aep: AEP
        """
        super().__init__(_aep, 'segmentationservice')
    
    def create_segmentjob(self, config_path: str, arg_replacements: Dict) -> SegmentJob:
        """ Creates an instance of Segmentjob through a post request.

        :param config_path: path to config yaml.
        :type config_path: str
        :param arg_replacements: values to replace in config.
        :type arg_replacements: Dict
        :return: An instance of segmentjob, representing a segmentjob on AEP.
        :rtype: SegmentJob
        """
        return self._create_aepobject(SegmentJob, config_path, arg_replacements)

    def get_segmentjob(self, id: str) -> SegmentJob:
        """ Retrieves an existing segmentjob from AEP.

        :param id: The id of the segmentjob
        :type id: str
        :return: Instance that represents the segmentjob on AEP. 
        :rtype: SegmentJob
        """
        return self._get_aepobject(SegmentJob, id)