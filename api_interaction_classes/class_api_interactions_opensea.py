import logging
import requests
import json
import time
import pandas as pd
from urllib.parse import urlunsplit, urlencode
from typing import Union
from class_timer import Timer
from class_percent_tracker import PercentTracker


class OpenseaAPIinteractions:
    """
        This class provides the ability to interact with the Opensea API.
    """

    # URL components
    __api_scheme = 'https'
    __api_network_location = 'api.opensea.io'

    # default headers
    __request_headers = {
        "Accept": "application/json"
    }

    def __init__(self, rate_limit: int = 0.5):
        """Initialize an instance of the class.
          Args:
            rate_limit: the maximum number of calls that one would like to make to the API per second.
              For example, if 1500 requests per minute are allowed, that would be
              25 per second, so this parameter would be set to 25. By default I'm setting it really slow, because
              OpenSea is not very generous with its free API and I don't want to get blocked.
          """

        # start the timer that keeps track of whether an API call is allowed
        # yet or not based on the rate limit
        self.__timer = Timer(rate_limit=rate_limit)
        self.__timer.reset()

        # start a python requests session
        self.__sesh = requests.session()
        self.__reset_headers()
    # ------------------------ END FUNCTION ------------------------ #

    def get_an_nft_tokens_metadata(self,
                                   contract_address: str,
                                   custom_token_identifier: str,
                                   api_format: str = "json") -> dict:
        """
          This method gets the metadata for a specific token in a contract.
          Args:
              contract_address: the address of the NFT contract
              custom_token_identifier: this is NOT the tokenId. This is a weird custom identifier
                that Opensea sets, which is different from the tokenId. It is found in the value
                returned for each tokens URI
              api_format: can be set to json (in the GIU, opensea gives two options: json or api)
          Returns:
              A dictionary with the data.
        """
        api_path = f"/api/v1/metadata/{contract_address}/{custom_token_identifier}"

        dict_api_query_params = {
            #"chain": chain
        }
        if api_format:
            dict_api_query_params["format"] = api_format

        return self.__make_one_api_call(self.__api_network_location,
                                        api_path,
                                        dict_api_query_params)
    # ------------------------ END FUNCTION ------------------------ #

    def get_many_nft_tokens_metadata(self,
                                     contract_address: str,
                                     opensea_token_uri_identifiers: Union[list, set, tuple, pd.Series],
                                     api_format: str = "json") -> pd.DataFrame:
        """
          This method gets the metadata for several NFT tokens.
          Args:
              contract_address: the account that one is interested in looking at the transactions for (usually
                an EOA, but not necessarily.)
              opensea_token_uri_identifiers: any iterable where each item is the odd identifier that opensea
                puts at the end of each token URI path.
              api_format: can be set to json (in the GIU, opensea gives two options: json or api)
          Returns:
              A pandas dataframe with the data.
        """

        list_of_tokens = []
        percent_tracker = PercentTracker(len(opensea_token_uri_identifiers), int_output_every_x_percent=5)
        counter = 0
        for item in opensea_token_uri_identifiers:
            list_of_tokens.append(
                self.get_an_nft_tokens_metadata(contract_address,
                                                item,
                                                api_format)
            )
            counter += 1
            percent_tracker.update_progress(counter,
                                            str_description_to_include_in_logging="Getting a list of NFT's metadata.")
        return pd.DataFrame(list_of_tokens)
    # ------------------------ END FUNCTION ------------------------ #

    def __make_one_api_call(self,
                            api_endpoint: str,
                            api_url_path: str,
                            dict_api_parameters: dict = {},
                            call_type: str = 'get',
                            call_body: str = '',
                            log_timing: bool = True) -> Union[None, dict]:
        """
          Make 1 call (pagination should be handled by the caller).
          Args:
              api_endpoint: This is equivalent, in URL terminology, as the 'network location'. For
               example "api.opensea.io"
              api_url_path: the path in the URL call to the API
              dict_api_parameters: a dictionary representing the parameters to be sent to the api as a query,
                so all the filters, the sorting, the fields to include, etc.
                In other words, all the things that usually go in a URL after the '?' and all the '&'
              call_type: the type of call that should be made. Eg. 'get' or 'post' or 'delete' or 'put'
              call_body: if the call_type is 'post', then generally there will be some data to be sent along
                with the post. That data can be provided as a string in this parameter.
              log_timing: boolean indicating whether this method should output to the logging how long it
                takes to make the API call.
          Returns:
              The object that is returned by the api.
        """
        start_time = time.time()

        # the urlunsplit method expects five arguments. Below, they are self-explanatory, except for the
        # fifth argument. In the fifth argument, a 'fragment' of a URL (I believe it is a portion that goes after
        # a '#' hash symbol) can be passed. We don't need that, so we just pass the empty string. I'm not sure
        # why a double parentheses is needed to call the function.
        api_url = urlunsplit((self.__api_scheme,
                              api_endpoint,
                              api_url_path,
                              urlencode(dict_api_parameters),
                              ""))
        api_method = None
        api_method_args = tuple()
        raw_response = None
        response_object = None

        dict_method_and_args = self.__organize_api_call_method_and_arguments(api_url, call_type, call_body)
        if dict_method_and_args:
            api_method = dict_method_and_args["method"]
            api_method_args = dict_method_and_args["args"]
        else:
            return

        self.__timer.wait_until_allowed(include_reset=True)
        try:
            raw_response = api_method(*api_method_args)
        except Exception as e:
            logging.warning('Something went wrong while retrieving data from ' + api_url
                            + ' -- The Exception was: ' + repr(e))
            return

        if raw_response.content:
            try:
                response_object = json.loads(raw_response.content)
            except Exception as e:
                logging.warning('Something went wrong while extracting the received data.'
                                + ' -- The Exception was: ' + repr(e))

        end_time = time.time()

        if log_timing:
            logging.debug(f"Time to execute the method that makes ONE single "
                          f"API call: {end_time - start_time} seconds")

        return response_object
    # ------------------------ END FUNCTION ------------------------ #

    def __organize_api_call_method_and_arguments(self, api_url: str, call_type: str, call_body: str) -> dict:
        """
        Pylama complained that the method above called __make_one_api_call was too complex,
        so I took this part of the code out of that method. It creates a dictionary
        which tells subsequent code which method of the python 'requests' library to call
        and the arguments to use when calling that method.
        Args:
            api_url: the URL that is going to be called to make the API call
            call_type: can be 'get' / 'post' / 'delete' / 'put'
            call_body: calls that are 'post' or 'put' typicaly also require that some body of
                data be sent along with the call. This parameter represents that data.
        Returns:
            A dictionary with two key/value pairs as follows
                {
                    "method": reference_to_method_to_be_called
                    "args": a_tuple_with_the_args_for_the_method
                }
        """
        dict_to_return = {}
        api_method = None
        api_method_args = ()

        if call_type == 'get':
            api_method = self.__sesh.get
        elif call_type == 'post':
            api_method = self.__sesh.post
        elif call_type == 'put':
            api_method = self.__sesh.put
        elif call_type == 'delete':
            api_method = self.__sesh.delete
        else:
            logging.error("Unexpected api call type passed in the 'call_type' argument to method "
                          "'__make_one_api_call'. API call is not being executed.")

        if api_method:
            if call_type == 'post' or call_type == 'put':
                api_method_args = (api_url, call_body)
            else:
                api_method_args = (api_url,)
            dict_to_return["method"] = api_method
            dict_to_return["args"] = api_method_args

        return dict_to_return
    # ------------------------ END FUNCTION ------------------------ #

    def __reset_headers(self):
        self.__sesh.headers.clear()
        self.__sesh.headers.update(self.__request_headers)
    # ------------------------ END FUNCTION ------------------------ #
