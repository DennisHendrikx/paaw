import os
import re
import yaml
from typing import List, Dict, Tuple

def parse_config(path: str =None, data: str =None, 
                 env_tag: str ='!ENV', arg_tag: str ='!ARG', 
                 arg_replacements: Dict ={}) -> Dict:
    """Parses a yaml file by replacing keywords. Keyswords are indicated by ${}.
    The !ENV tag indicates the keyword should be retrieved from environment variables.
    The !ARG tag indicates the keyword should be retrieved from the passes arg_replacement dictionary.

    :param path: Path to the yaml file, defaults to None
    :type path: str, optional
    :param data: Content of yaml file as a string, defaults to None
    :type data: str, optional
    :param env_tag: Tag for environment variables, defaults to '!ENV'
    :type env_tag: str, optional
    :param arg_tag: Tag for variables to be replaced by arg_replacements, defaults to '!ARG'
    :type arg_tag: str, optional
    :param arg_replacements: The key-value pairs to replace tagged variables with, defaults to {}
    :type arg_replacements: Dict, optional
    :raises ValueError: Raised when one of the variables can not be found.
    :return: A parses yaml file as a dictionary.
    :rtype: Dict
    """
    # pattern for global vars: look for ${word}
    pattern = re.compile('.*?\${(\w+)}.*?')
    loader = yaml.SafeLoader

    # the tag will be used to mark where to start searching for the pattern
    # e.g. somekey: !ENV somestring${MYENVVAR}blah blah blah
    loader.add_implicit_resolver(env_tag, pattern, None)
    loader.add_implicit_resolver(arg_tag, pattern, None)

    def constructor_env_variables(loader, node):
        """
        Extracts the environment variable from the node's value
        :param yaml.Loader loader: the yaml loader
        :param node: the current node in the yaml
        :return: the parsed string that contains the value of the environment
        variable
        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    f'${{{g}}}', os.environ.get(g, g)
                )
            return full_value
        return value

    def constructor_arg_variables(loader, node, arg_replacements=arg_replacements):
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    f'${{{g}}}', arg_replacements[g]
                )
            return full_value
        return value

    loader.add_constructor(env_tag, constructor_env_variables)
    loader.add_constructor(arg_tag, constructor_arg_variables)

    if path:
        with open(path) as conf_data:
            return yaml.load(conf_data, Loader=loader)
    elif data:
        return yaml.load(data, Loader=loader)
    else:
        raise ValueError('Either a path or data should be defined as input')