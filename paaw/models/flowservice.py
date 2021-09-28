from __future__ import annotations
from .abstractmodel import AEPCollection, AEPObject
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..aep import AEP

class Flow(AEPObject):
    name = 'flows'
    # for some reason, request to flows always returns a list with 1 item. 
    id_find_func = lambda self, definition: definition['items'][0]['id']
    def get_flowruns(self) -> List[FlowRun]:
        """ Retrieves all the flowruns for this flow

        :return: List of flowruns
        :rtype: List[FlowRun]
        """
        params = {'property': 'flowId=='+self.id}
        result = self._aep.get(
            'flowservice.runs',
            params=params)
        return [FlowRun(item, self._aep, item['id']) for item in result['items']]

    def start_flowrun(self) -> FlowRun:
        """Starts a new flowrun for this flow, using a post request.

        :return: The newly created flowrun.
        :rtype: FlowRun
        """
        body = {
            'status': 'active',
            'flowId': self.id
        }
        result = self._aep.post(
            'flowservice.runs',
            body=body,
            params={}
        )
        return self._aep.flow_service.get_flowrun(result["id"])


class FlowRun(AEPObject):
    name = 'runs'
    # for some reason, request to flowruns always returns a list with 1 item. 
    id_find_func= lambda self, definition: definition['items'][0]['id']
    def refresh_definition(self):
        """ Refreshed the definition of this flow. Usefull when polling the
        status of a flowrun
        """
        this_flowrun = self._aep.flow_service.get_flowrun(self.id)
        self.definition = this_flowrun.definition


class FlowService(AEPCollection):
    def __init__(self, _aep: AEP):
        """ A collection for endpoints under Flow Service
        See: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#!acpdr/swagger-specs/flow-service.yaml

        :param _aep: the top class though which requests are made.
        :type _aep: AEP
        """
        super().__init__(_aep, 'flowservice')
    
    def get_flow(self, flow_id: str) -> Flow:
        """ Retrieves a flow for a given flow_id.

        :param flow_id: the flow_id to retrieve.
        :type flow_id: str
        :return: The flow under flow_id.
        :rtype: Flow
        """
        return self._get_aepobject(Flow, flow_id)

    def create_flow(self, config_path: str, arg_replacements: Dict) -> Flow:
        """ Creates a new flow for a given config at config_path.

        :param config_path: Path to the config.yaml for the flow to be created.
        :type config_path: str
        :param arg_replacements: Replacements to be made in the config.yaml that describes the new
        flow.
        :type arg_replacements: Dict
        :return: The newly created flow.
        :rtype: Flow
        """
        return self._create_aepobject(Flow, config_path, arg_replacements)

    def get_all_flows(self, property_filter:str =None) -> List[Flow]:
        """ Retrieves all flows.

        :param property_filter: A string used to filter the flows to retrieve. For example: 
        flowId==some_flow_id, defaults to None
        :type property_filter: str, optional
        :return: A list of flows, satisfying the filter.
        :rtype: List[Flow]
        """
        if property_filter:
            params = {'property': property_filter}
        else:
            params = {}
        return self._get_aepobject_list(Flow, get_params=params)
    
    def get_flowrun(self, flowrun_id: str) -> FlowRun:
        """ Retrieves a flowrun for given flowrun_id.

        :param flowrun_id: The flowrun_id to retrieve.
        :type flowrun_id: str
        :return: The flowrun.
        :rtype: FlowRun
        """
        return self._get_aepobject(FlowRun, flowrun_id)
    
    def get_all_flowruns(self, property_filter:str =None) -> List[FlowRun]:
        """ Retrieves all flowruns

        :param property_filter: A string used to filter the flowruns. For example:
        flowId==some_flow_id, defaults to None
        :type property_filter: str, optional
        :return: A list of flowruns satisfying the filter.
        :rtype: List[FlowRun]
        """
        if property_filter:
            params = {'property': property_filter}
        else:
            params = {}
    
        return self._get_aepobject_list(FlowRun, get_params=params)