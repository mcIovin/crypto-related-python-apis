import logging
import requests
import json
import pandas as pd
# import xmltodict
import time
from pathlib import Path
from urllib.parse import urlunsplit, urlencode
from typing import Union
from helperclass_timer import Timer


class TatumAPIinteractions:
    """
        This class provides the ability to interact with the Tatum API.
    """

    # URL components
    __api_scheme = 'https'
    __api_network_location = 'api-us-west1.tatum.io'

    # default headers
    __request_headers = {
        "Accept": "text/html",
        "Accept-Language": "en-US,en;q=0.5",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0"
    }

    def __init__(self, full_path_to_credentials_file: Path, rate_limit: int = 5, page_size: int = 50):
        """Initialize an instance of the class.
          Args:
            full_path_to_credentials_file: a json formatted file (as a posix path) that has API key.
            rate_limit: the number of calls that one is allowed to make to the API per second. eg. 5
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

    def get_current_ethereum_block_number(self, testnet: str = "") -> int:
        """
            This method gets the current block number.
            NOTE: From my testing (not very thorough) it looks like the API
                will always return the current block of mainnet, IF the api key used
                is for mainnet - even if the headers specify a testnet. I'm guessing
                the converse is also true; likely if a testnet api key is used,
                the reply will be for testnet no matter what, but in that case, WHICH
                testnet can be specified.
            Args:
              testnet: If this value is ommitted, then mainnet is used. Possible values are "ethereum-ropsten",
                "ethereum-ropsten", "ethereum-rinkeby"
            Returns:
              The current block.
        """
        api_path = "/v3/ethereum/block/current"
        if testnet:
            header_to_add = {
                "x-testnet-type": testnet
            }
            self.__sesh.headers.update(header_to_add)

        block = self.__make_one_api_call(self.__api_network_location,
                                         api_path)

        if testnet:
            # in case other methods still need to use the requests session, we'll reset the headers,
            # which in this case means removing the 'testnet' header if it was provided
            self.__reset_headers()

        return block
    # ------------------------ END FUNCTION ------------------------ #

    def get_multi_token_transactions_by_address(self,
                                                account_address: str,
                                                contract_address: str,
                                                chain: str = "ETH",
                                                from_block: int = -1,
                                                to_block: int = -1) -> pd.DataFrame:
        """
          This method gets all the transactions for a multitoken type of contract (example, on Ethereum
          an ERC1155 type of contract) for a specific address that has interacted with that contract.
          Args:
              account_address: the account that one is interested in looking at the transactions for (usually
                an EOA, but not necessarily.)
              contract_address: the address of the contract on the blockchain.
              chain: a string that represents the chain one is interested in.
              from_block: the starting block, eg. one is interested only in transactions that happened
                after block number X
              to_block: if one is only interested in transactions up to a certain point, this parameter
                can be used to specify an upper limit on the blocks.
          Returns:
              A pandas dataframe with the data.
        """
        api_path = f"/v3/multitoken/transaction/{chain}/{account_address}/{contract_address}"
        dict_api_query_params = {
            "pageSize": self.__page_size
        }
        if from_block > -1:
            dict_api_query_params["from"] = from_block
        if to_block > -1:
            dict_api_query_params["to"] = to_block

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
               example "api-us-west1.tatum.io"
              api_url_path: the path in the URL call to the API as specified by Tatum documentation. For
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
        
        there_are_likely_more_results = True
        dict_api_query_parameters['offset'] = 0
        full_data_set = []
        page_counter = 1

        while there_are_likely_more_results:
            # # check to make sure a variable on disk hasn't been told that the program should stop
            # if not self.__var_mgr.var_retrieve(self.__str_execution_may_go_on_jobs):
            #     return

            one_page_data = self.__make_one_api_call(api_endpoint,
                                                     api_url_path,
                                                     dict_api_parameters=dict_api_query_parameters)
            # Tatum does not provide any pagination info in its response
            # so all we can do is check if the next page still has data in it
            if one_page_data:
                # add the result to the running tally
                full_data_set.extend(one_page_data)
                dict_api_query_parameters['offset'] += num_items_per_page
                if log_progress:
                    logging.info(f"Fetching page {page_counter}.")
                page_counter += 1
            else:
                there_are_likely_more_results = False

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
               example "api-us-west1.tatum.io"
              api_url_path: the path in the URL call to the API as specified by Tatum documentation. For
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
            raw_response = api_method(*api_method_args, timeout=30)
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
        self.__sesh.headers.update({"x-api-key": dict_creds['api_key_free_mainnet']})
    # ------------------------ END FUNCTION ------------------------ #
