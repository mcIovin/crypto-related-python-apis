import logging
import requests
import json
import pandas as pd
import time
from pathlib import Path
from urllib.parse import urlunsplit, urlencode
from typing import Union
from class_timer import Timer
from class_percent_tracker import PercentTracker


class MoralisAPIinteractions:
    """
        This class provides the ability to interact with the Moralis API.
    """

    # URL components
    __api_scheme = 'https'
    __api_network_location = 'deep-index.moralis.io'

    # default headers
    __request_headers = {
        "Accept": "application/json"
    }

    def __init__(self, full_path_to_credentials_file: Path, rate_limit: int = 25, page_size: int = 50):
        """Initialize an instance of the class.
          Args:
            full_path_to_credentials_file: a json formatted file (as a posix path) that has API key.
            rate_limit: the number of calls that one is allowed to make to the API per second. On the free
              account, the moralis documentation says 1500 requests per minute are allowed, which is
              25 per second, so this is what I've set the default to.
            page_size: the number of items to get from the API, for calls where information
                is being retrieved, and the API returns a certain number of objects per
                page.
          """
        self.__path_to_file_with_creds = full_path_to_credentials_file
        self.__page_size = page_size
        # start the timer that keeps track of whether an API call is allowed
        # yet or not based on the rate limit
        self.__timer = Timer(rate_limit=rate_limit)
        self.__timer.reset()

        # start a python requests session
        self.__sesh = requests.session()
        self.__reset_headers()
    # ------------------------ END FUNCTION ------------------------ #

    def get_nft_transfers(self,
                          address: str,
                          chain: str = "eth",
                          format: str = "",
                          direction: str = "") -> pd.DataFrame:
        """
          This method gets all the NFT transactions that a particular address has been involved in (sending and/or
          receiving.
          Args:
              address: the account that one is interested in looking at the transactions for (usually
                an EOA, but not necessarily.)
              chain: a string that represents the chain one is interested in. Eg, eth, ropsten, matic, etc.
              format: 'decimal' or 'hex' (decimal is default).
              direction: 'to', 'from', or 'both' (both is the default)
          Returns:
              A pandas dataframe with the data.
        """
        api_path = f"/api/v2/{address}/nft/transfers"

        dict_api_query_params = {
            "chain": chain
        }
        if format:
            dict_api_query_params["format"] = format
        if direction:
            dict_api_query_params["direction"] = direction

        return self.__get_full_data_set_from_api(self.__api_network_location,
                                                 api_path,
                                                 dict_api_query_params)
    # ------------------------ END FUNCTION ------------------------ #

    def __get_full_data_set_from_api(self,
                                     api_endpoint: str,
                                     api_url_path: str,
                                     dict_api_query_parameters: dict,
                                     num_items_per_page: int = 50,
                                     return_as: str = 'dataframe',
                                     log_progress: bool = True) -> Union[list, pd.DataFrame, None]:
        """
          This method is .
          Args:
              api_endpoint: This is equivalent, in URL terminology, as the 'network location'. For
               example "deep-index.moralis.io"
              api_url_path: the path in the URL call to the API as specified by Moralis documentation. For
                example to get a list of transactions an address has performed on a particular contract:
                "/v3/multitoken/transaction/{chain}/{address}/{tokenAddress}"
              dict_api_parameters: a dictionary representing the parameters to be sent to the api as a query,
                so all the filters, the sorting, the fields to include, etc.
                In other words, all the things that usually go in a URL after the '?' and all the '&'
              return_as: how the data should be returned. Valid options are:
                - 'dataframe' -> returns a pandas dataframe
                - 'list' -> returns a list of dictionaries containing the data
                (Each row in the dataframe has the same data as each dictionary in the list.)
              log_progress: should this method send to 'logging' percentage progress updates.
          Returns:
              A list of dictionaries containing the response from the API, or a dataframe if the correct
              flag is set in the method's parameters.
        """

        dict_api_query_parameters['limit'] = num_items_per_page
        full_data_set = []
        there_are_likely_more_results = True
        is_first_loop_iteration = True
        # Declare a 'dummy' percent tracker object, which will be redefined below
        # once the size of the dataset is known. However, we declare it here so
        # things like pylama/pycharm won't complain about it maybe being used
        # without being declared, as they would if we only declared it within an IF.
        percent_tracker = ''

        while there_are_likely_more_results:
            api_response = self.__make_one_api_call(api_endpoint,
                                                    api_url_path,
                                                    dict_api_parameters=dict_api_query_parameters)
            # After the first iteration of the loop, we get our first response from the API
            # which allows us to get information like the size of the dataset, which allows
            # us to setup a percent tracker, for example.
            if is_first_loop_iteration:
                size_data_set = api_response['total']
                if size_data_set > 0:
                    percent_tracker = PercentTracker(api_response['total'])
                is_first_loop_iteration = False

            # There are two ways we can tell if there are more results that need to be fetched:
            # When all the results have been returned, the list at api_response['result'] will be empty,
            # and the cursor at api_response['cursor'] will be the empty string.
            # Either can be used to determine if the loop should end. One can also use the number of items
            # per ages in conjunctions with 'offset' to do pagination, but cursors get rid of the risk of
            # missing/duplicate items when a data set might be changing.
            cursor = api_response['cursor']
            if cursor:
                # add the result to the running tally
                full_data_set.extend(api_response['result'])
                dict_api_query_parameters["cursor"] = cursor
            else:
                there_are_likely_more_results = False

            if percent_tracker:
                percent_tracker.update_progress(len(full_data_set),
                                                str_description_to_include_in_logging="done retreiving data from"
                                                                                      "the current Moralis API call")

        if 'dataframe' in return_as:
            return pd.DataFrame(full_data_set)
        elif 'list' in return_as:
            return full_data_set
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
               example "deep-index.moralis.io"
              api_url_path: the path in the URL call to the API as specified by Moralis documentation. For
                example to get a list of transactions an address has performed on a particular contract:
                "/v3/multitoken/transaction/{chain}/{address}/{tokenAddress}"
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
        with open(self.__path_to_file_with_creds, mode='r') as creds_file:
            dict_creds = json.load(creds_file)
        self.__sesh.headers.clear()
        self.__sesh.headers.update(self.__request_headers)
        self.__sesh.headers.update({"x-api-key": dict_creds['api_key']})
    # ------------------------ END FUNCTION ------------------------ #
