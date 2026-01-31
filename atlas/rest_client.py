import time
import requests

from atlas.config import (
    DUMMYJSON_BASE_URL,
    API_PAGE_SIZE,
    API_MAX_RETRIES,
    API_BACKOFF_SECONDS,
    API_AUTH_TOKEN,
)

class RestAPIClient:
    """
    Generic Rest Api Client that supports:
    - Pagination
    - retries with backoff
    -optional authentication
    """

    def __init__(self, base_url: str = DUMMYJSON_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()

        self.headers = {}
        if API_AUTH_TOKEN:
            self.headers["Authorization"] = f"Bearer {API_AUTH_TOKEN}"

    def _get(self,params:dict)->dict:
        """internal get method with retires and backoff handling"""
        for atempt in range(API_MAX_RETRIES):
            try:
                response=self.session.get(
                    self.base_url,
                    params=params,
                    headers=self.headers,
                    timeout=10
                )
                # handle rate limiting

                if response.status_code==429:
                    time.sleep(API_BACKOFF_SECONDS )
                    continue
                response.raise_for_status()
                return response.json()
            
            except requests.RequestException :
                if atempt ==API_MAX_RETRIES -1:
                    raise
                time.sleep(API_BACKOFF_SECONDS)

 
    def fetch_paginated(self,data_key:str,extra_params:dict | None = None):
        """
        Generic pagination generator
        
        parameters:
        - data_key : the key in json response that holds records (eg "products")

        """
        skip =0
        extra_params = extra_params or {} 

        while True:
            params = {
                "limit":API_PAGE_SIZE,
                "skip":skip,
                **extra_params
            }

            data = self._get(params)
            records = data.get(data_key,[])
            total = data.get("total",0)

            if not records:
                break
            for record in records:
                yield record
            skip+=API_PAGE_SIZE
            if skip >= total:
                break   

