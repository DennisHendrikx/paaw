from dictor import dictor
import sys
import re
import json
import datetime
from jwt import JWT, jwk_from_pem
from .general_utils import setup_logger, http_request
import os
from urllib.parse import urlencode
import base64


LOGGER = setup_logger(__name__)

def get_jwt_token(ims_host, org_id, tech_acct, api_key, priv_key):
    """
    :param ims_host: ims host
    :param org_id: org id
    :param tech_acct: technical account ID (obtained from Adobe IO integration)
    :param api_key: api key (obtained from Adobe IO integration)
    :param priv_key: private key counter part to the public key which was used for creating Adobe IO integration
    :return: encoded jwt token
    """

    # create payload
    payload = {
        "exp": token_expiration_millis(),
        "iss": org_id,
        "sub": tech_acct,
        "https://" + ims_host + "/s/" + "ent_dataservices_sdk": True,
        "aud": "https://" + ims_host + "/c/" + api_key
    }

    # create JSON Web Token
    instance = JWT()
    signing_key = jwk_from_pem(priv_key)
    jwt_token = instance.encode(payload, signing_key, alg='RS256')

    LOGGER.debug("encoded jwt_token = %s", jwt_token)
    return jwt_token


def get_access_token(ims_host, ims_endpoint_jwt, org_id, tech_acct, api_key,
                     client_secret, priv_key):
    """
    :param ims_host: ims host
    :param ims_endpoint_jwt: endpoint for exchange jwt
    :param org_id: org id
    :param tech_acct: technical account ID (obtained from Adobe IO integration)
    :param api_key: api key (obtained from Adobe IO integration)
    :param client_secret: client secret (obtained from Adobe IO integration)
    :param priv_key : private key
    :return: access token for the apis
    """
    url = "https://" + ims_host + ims_endpoint_jwt

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cache-Control": "no-cache"
    }

    body_credentials = {
        "client_id"     : api_key,
        "client_secret" : client_secret,
        "jwt_token"     : get_jwt_token(ims_host, org_id, tech_acct, api_key, priv_key)
    }
    body = urlencode(body_credentials)

    # send http post request
    res_text = http_request("post", url, headers, body)
    access_token = json.loads(res_text)["access_token"]
    LOGGER.debug("access_token: %s", access_token)
    return access_token


def token_expiration_millis():
    """
    :return: token expiration in milliseconds
    """
    dt = datetime.datetime.now()
    return int(dt.timestamp()*1000) + 24 * 60 * 60 * 1000

def get_token(cfg):
    """
    :return: ims token for authorization
    """
    ims_token = dictor(cfg, "Platform" + ".ims_token", checknone=True)
    api_key = dictor(cfg, "Enterprise" + ".api_key", checknone=True)
    org_id = dictor(cfg, "Enterprise" + ".org_id", checknone=True)

    if ims_token == "<ims_token>":
        # Server parameters
        ims_host = dictor(cfg, "Server" + ".ims_host", checknone=True)
        ims_endpoint_jwt = dictor(cfg, "Server" + ".ims_endpoint_jwt", checknone=True)

        # Enterprise parameters used to construct JWT
        client_secret = dictor(cfg, "Enterprise" + ".client_secret", checknone=True)
        tech_acct = dictor(cfg, "Enterprise" + ".tech_acct", checknone=True)

        # read private key from config
        priv_key=dictor(cfg, "Enterprise" + ".priv_key", checknone=True)
        priv_key_bytes = base64.b64decode(priv_key.encode('ascii'))
        ims_token = "Bearer " + get_access_token(ims_host, ims_endpoint_jwt, org_id, tech_acct, api_key,
                                                 client_secret, priv_key_bytes)
    if not ims_token.startswith("Bearer "):
        ims_token = "Bearer " + ims_token

    return ims_token

def get_headers(cfg):
    """
    :return: headers
    """
    api_key = dictor(cfg, "Enterprise" + ".api_key", checknone=True)
    org_id = dictor(cfg, "Enterprise" + ".org_id", checknone=True)
    sandbox_name = dictor(cfg, "Titles" + '.sandbox_name', default="prod")
    platform_gateway = dictor(cfg, "Platform" + ".platform_gateway", checknone=True)
    headers = {}
    ims_token = get_token(cfg)
    if ims_token is not None:
        if platform_gateway == 'https://platform.adobe.io': # AEP
            headers = {
                "Authorization": ims_token,
                "x-api-key": api_key,
                "x-gw-ims-org-id": org_id,
                'x-sandbox-name': sandbox_name
            }
        elif platform_gateway == 'https://mc.adobe.io': # ACS
            headers = {
                "Authorization": ims_token,
                "x-api-key": api_key,                
            }
        else:
            raise Exception(f"Not able to set headers for unkown platform_gateway: {platform_gateway}.")

    return headers