from __future__ import annotations
from .abstractmodel import AEPCollection, AEPObject
from typing import List, Dict, Tuple, TYPE_CHECKING
if TYPE_CHECKING:
    from ..aep import AEP
import io
import pandas as pd
import pyarrow.parquet as pq

class DataSetFile(AEPObject):
    name = 'datasetfile'
    id_find_func = lambda self, definition: definition['dataSetFileId']
    def get_pathnames(self):
        """ Uses a get request to retrieve the pathnames to the underlying files

        :return: List of pathnames
        :rtype: List
        """
        result = self._aep.get(
            path='dataaccess.files',
            body={},
            params={},
            url_suffix='/'+self.id
        )
        pathnames = [data['name'] for data in result['data']]
        return pathnames

    def get_file_at_pathname(self, pathname):
        """ Gets a BytesIO filehandle to bytes sitting in memory for the passed
        pathname. The pathname describes the path to one file in this DataSetFile.

        :param pathname: The pathname to the file
        :type pathname: string
        :return: BytesIO filehandle of the file as bytes in memory
        :rtype: BytesIO
        """
        filetype = pathname.split('.')[-1]
        if filetype == 'parquet':
            url_suffix = '/'+self.id
            base_url, extra_headers = self._aep._path_to_endpoint_and_headers('dataaccess.files')
            url = base_url+url_suffix
                
            resp = self._aep.session.request(
                method='GET', 
                url=url, data={}, 
                params={'path': pathname}, 
                headers=extra_headers)
            file = io.BytesIO(resp.content)
        else:
            raise Exception(f'The filetype {filetype} is not implemented')
        return file

    def get_all_files(self):
        """Loops over all the pathnames under this DataSetFile, and retrieves
        the underlying file as a byte-array in memory.

        :return: Dictionary with as keys the pathnames, as values the BytesIO handle
        to the in-memory byte-array.
        :rtype: Dict[str, BytesIO]
        """
        pathnames = self.get_pathnames()
        files = {}
        for pathname in pathnames:
            file = self.get_file_at_pathname(pathname)
            files[pathname]=file
        return files

    def get_all_files_as_dataframes(self):
        """Gets all files and converts them to a pandas dataframe

        :return: A dictionary keyed on pathname containing the dataframes
        :rtype: Dict[str, pandas.DataFrame]
        """
        files = self.get_all_files()
        frames = {}
        for pathname, file in files.items():
            frames[pathname] = pd.read_parquet(file)
        return frames

    def get_all_files_as_arrowtable(self):
        """ Gets all files and converts them to arrow tables. 
        For writing to a database, avoiding parsing to pandas is most likely
        less memory intensive.

        :return: A dictionary keyed on pathname containing the arrow tables
        :rtype: Dict[str, pyarrow.Table]
        """
        files = self.get_all_files()
        arrow_tables = {}
        for pathname, file in files.items():
            arrow_tables[pathname] = pq.read_table(source=file)
        return arrow_tables

#TODO: change this to iterators to prevent memory issues?    
    def get_all_files_as_dicts(self):
        """ Gets all files as python dictionaries. This is done by retrieving
        the files as parquet, converting this to arrow tables and converting the
        arrow tables to dictionaries.

        :return: A dictionary keyed on pathname containing a list of dictionaries.
        For one pathname, we have a parquet file which is converted to an arrow table.
        Each item in the list per pathname is a single batch withing the arrow table converted
        to a python dictionary.
        :rtype: Dict[str, List[Dict]]
        """
        pyarrow_tables = self.get_all_files_as_arrowtable()
        dicts = {}
        for pathname, table in pyarrow_tables.items():
            dicts[pathname] = []
            for batch in table.to_batches():
                dicts[pathname].append(batch.to_pydict())
        return dicts

class DataAccess(AEPCollection):
    def __init__(self, _aep: AEP):
        """ A collection for endpoints under DataAccess.
        See: https://www.adobe.io/apis/experienceplatform/home/api-reference.html#!acpdr/swagger-specs/catalog.yaml

        :param _aep: the top class though which requests are made.
        :type _aep: AEP
        """
        super().__init__(_aep, 'dataaccess')
