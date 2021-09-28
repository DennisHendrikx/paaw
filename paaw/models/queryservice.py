from __future__ import annotations
import requests
from ..exc import NotPossibleToUpdateQuery
from requests.api import request
from .abstractmodel import AEPCollection, AEPObject
from typing import List, Dict, Tuple, TYPE_CHECKING
import re
if TYPE_CHECKING:
    from ..aep import AEP


class Query(AEPObject):
    name = 'query'


class ScheduledQuery(AEPObject):
    name = 'scheduledquery'

    def refresh_definition(self):
        """ Gets the new definition of this scheduledquery through a get request.
        The status of a query can change over time. When it is first created it's preparing
        and than goes into active. Use this to refresh the definition so it matches
        the underlying artifact on AEP again. 
        """
        result = self._aep.get(path='queryservice.scheduledquery',
                              body={},
                              params={},
                              url_suffix='/'+self.id)
        self.definition = result

    def patch_obj(self, path: str, value: str):
        """ A patch request to the definition of a scheduledquery.
        Specified by which value to patch on which path.

        :param path: Path to value.
        :type path: str
        :param value: The replacement value.
        :type value: str
        """
        body = {
            "body": [{
                "op": "replace",
                "path": path,
                "value": value
            }]
        }
        self._aep.update(path='queryservice.scheduledquery', 
                         body=body, 
                         params={}, 
                         url_suffix='/'+self.id)
        self.refresh_definition()
    
    def change_state(self, new_state: str):
        """ Changes the state of the query.

        :param new_state: The new state.
        :type new_state: str
        """
        self.patch_obj(path='/state', value=new_state)
    
    def change_schedulecron(self, new_schedule: str):
        """ Updates the schedule cron string.

        :param new_schedule: The new cron string.
        :type new_schedule: str
        """
        self.patch_obj(path='schedule/schedule', value=new_schedule)
    
    def update_this_def(self, new_def: Dict):
        """ Updates the scheduledquery with a new definition.

        :param new_def: the new definition of the scheduledquery.
        :type new_def: Dict
        :raises NotPossibleToUpdateQuery: Raised when the updated definition
        changes the query. In that case, the query needs to be redeployed.
        """
        if self.definition['query'] != new_def['query']:
            raise NotPossibleToUpdateQuery("Query changed. Please delete this scheduledquery and deploy the new definition")
        elif not new_def['schedule'].items() <= self.definition['schedule'].items():
            print("change in schedule, updating existing")
            self.change_schedulecron(new_def['schedule']['schedule'])
        else:
            print("no changes detected")

    def delete(self):
        """ Deletes this scheduledquery by first disabling it. Sets id and definition
        to none to signify it's been deleted.
        """
        try:
            self.change_state('disable')
        except requests.exceptions.HTTPError as e:
            print('Encountered error {}. Trying again later often works'.format(e))
        self._aep.delete(path='queryservice.scheduledquery', 
                         body={}, 
                         params={}, url_suffix='/'+self.id)
        self.id = None
        self.definition = None


class QueryService(AEPCollection):
    def __init__(self, _aep: AEP):
        """ Collection of endpoints under query service.
        See: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#!acpdr/swagger-specs/qs-api.yaml

        :param _aep: Top class through which requests are made.
        :type _aep: AEP
        """
        super().__init__(_aep, 'queryservice')
    
    def create_query(self, config_path: 'str', arg_replacements: Dict) -> Query:
        """ Creates query

        :param config_path: path to config yaml
        :type config_path: str
        :param arg_replacements: replacement values in config.
        :type arg_replacements: Dict
        :return: An instance of query, which represents a query on AEP.
        :rtype: Query
        """
        return self._create_aepobject(Query, config_path, arg_replacements)
    
    def create_scheduledquery(self, config_path: str, arg_replacements: Dict) -> ScheduledQuery:
        """ Creates a scheduled query.

        :param config_path: path to config yaml.
        :type config_path: str
        :param arg_replacements:  replacement values in config.
        :type arg_replacements: Dict
        :return: An instance of scheduledquery, which represents a scheduled query on AEP.
        :rtype: ScheduledQuery
        """
        return self._create_aepobject(ScheduledQuery, config_path, arg_replacements)    

    def get_query(self, id: str) -> Query:
        """ Creates a query object from an existing query on AEP.

        :param id: The id of the query on AEP.
        :type id: str
        :return: And instance of Query, which represents a query on AEP.
        :rtype: Query
        """
        return self._get_aepobject(Query, id)
    
    def get_scheduledquery(self, id: str) -> ScheduledQuery:
        """ Creates a Scheduled query object from an existing schedules 
        query on AEP.

        :param id: The id of the query on AEP.
        :type id: str
        :return: And instance of ScheduledQuery, which represents a query on AEP.
        :rtype: ScheduledQuery
        """
        return self._get_aepobject(ScheduledQuery, id)

    def get_list_scheduledqueries_by_name(self, name: str) -> List:
        """Creates a list of Scheduled query objects that contain a certain string in the name.
        The search is case insensitive.

        :param name: The string to match the query name on.
        :type name: str
        :return: A list of ScheduledQuery objects.
        :rtype: ScheduledQuery
        """
        result = self._aep.get(path='queryservice.scheduledquery', params={}, body={}, url_suffix='')
        query_list = []
        if len(result['schedules'])==0:
            raise Exception('No matching ScheduledQuery objects have been found.')
        for item in result['schedules']:
            matched = re.match(r'.*({}).*'.format(name).lower(), item['query']['name'].lower())
            is_match = bool(matched)
            if is_match is True:
                query_id = item['id']
                query_list.append(self._get_aepobject(ScheduledQuery, query_id))
        if len(query_list)==0:
            raise Exception('No matching ScheduledQuery objects have been found with the name {}'.format(name))
        return query_list


