from os import getenv
import logging
import json
import time
import requests
import pandas as pd
from urllib.parse import urlunsplit, urlencode
from typing import Union
from helperclass_timer import Timer
from helperclass_percent_tracker import PercentTracker


class MoralisAPIinteractions:
    """
        This class provides the ability to interact with the Moralis API.
        NOTE - this class expects to be able to access the Moralis API key in an environment
        variable called 'MORALIS_KEY'.
    """

    # URL components
    __api_scheme = 'https'
    __api_network_location = 'deep-index.moralis.io'

    # default headers
    __request_headers = {
        "Accept": "application/json"
    }

    def __init__(self, rate_limit: int = 1, page_size: int = 50):
        """Initialize an instance of the class.
        NOTE - this class expects to be able to access the Moralis API key in an environment
        variable called 'MORALIS_KEY'.
          Args:
            rate_limit: the number of calls that one is allowed to make to the API per second. On the free
              account, the moralis documentation says 1500 requests per minute are allowed, which is
              25 per second; however, each API has its own 'cost'. For example each call about NFT
              transfers has a 'cost' of 5 API calls. So there is a body of work that could be done here
              to make each type of call 'limited' within this class to exactly how many calls are allowed.
              For now, I've just set the default limit to be low enough that it should work.
            page_size: the number of items to get from the API, for calls where information
                is being retrieved, and the API returns a certain number of objects per
                page.
          """
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

    def get_nfts_in_a_contract(self,
                          address: str,
                          chain: str = "eth",
                          format: str = "") -> pd.DataFrame:
        """
          This method gets all the NFTs that exist in a contract according to the Moralis API.
          Args:
              address: the account that one is interested in looking at the transactions for (usually
                an EOA, but not necessarily.)
              chain: a string that represents the chain one is interested in. Eg, eth, ropsten, matic, etc.
              format: 'decimal' or 'hex' (decimal is default).
          Returns:
              A pandas dataframe with the data.
        """
        api_path = f"/api/v2/nft/{address}"

        dict_api_query_params = {
            "chain": chain
        }
        if format:
            dict_api_query_params["format"] = format

        return self.__get_full_data_set_from_api(self.__api_network_location,
                                                 api_path,
                                                 dict_api_query_params)
    # ------------------------ END FUNCTION ------------------------ #

    def resync_an_nft_tokens_metadata(self,
                                      contract_address: str,
                                      token_id: str,
                                      chain: str = "eth",
                                      log_api_response_message: bool = False):
        """
          This method gets all the NFT transactions that a particular address has been involved in (sending and/or
          receiving.
          Args:
              contract_address: the address of the NFT contract
              token_id: the id of the NFT for which to resync the metadata
              chain: a string that represents the chain one is interested in. Eg, eth, ropsten, matic, etc.
              log_api_response_message: whether the api's response should be shown on the console or not,
                which can be quite useful, but also quite noisy if querying repeatedly.
        """
        api_path = f"/api/v2/nft/{contract_address}/{token_id}/metadata/resync"

        dict_api_query_params = {
            "chain": chain,
            "flag": "uri",
            "mode": "sync"
        }

        response = self.__make_one_api_call(self.__api_network_location,
                                            api_path,
                                            dict_api_query_params)
        there_was_a_problem = False
        if 'status' not in response:
            there_was_a_problem = True
        else:
            if response['status'] != 'completed':
                there_was_a_problem = True

        if there_was_a_problem:
            logging.warning(f"Did not receive the expected 'completed' response when resyncing metadata "
                            f"for token_id {token_id}")

        if log_api_response_message:
            logging.info(response)
    # ------------------------ END FUNCTION ------------------------ #

    def resync_many_nft_tokens_metadata(self,
                                        contract_address: str,
                                        token_ids: Union[list, set, tuple, pd.Series],
                                        chain: str = "eth",
                                        log_api_response: bool = False,
                                        sleep_time_between_requests: float = 0):
        """
          This method gets the metadata for several NFT tokens.
          Args:
              contract_address: the account that one is interested in looking at the transactions for (usually
                an EOA, but not necessarily.)
              token_ids: any iterable where each item is a string representing a token id.
              chain: a string that represents the chain one is interested in. Eg, eth, ropsten, matic, etc.
              log_api_response: whether the api's response should be shown on the console or not,
                which can be quite useful, but also quite noisy if querying repeatedly.
              sleep_time_between_requests: resyncing NFT metadata seems to trigger a more sensitive rate limit
                than other functions. This is based on experience; I can't find this in their documentation. So
                this parameter allows for a certain amount of seconds (or fraction thereof) to be waited for, before
                resyncing the metadata of the next token in the iterable.
        """
        percent_tracker = PercentTracker(len(token_ids), int_output_every_x_percent=5)
        counter = 0
        for item in token_ids:
            self.resync_an_nft_tokens_metadata(contract_address, item, chain, log_api_response)
            if sleep_time_between_requests:
                time.sleep(sleep_time_between_requests)
            counter += 1
            percent_tracker.update_progress(counter,
                                            show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging="Resyncing metadata for a list "
                                                                                  "of NFT tokens.")
    # ------------------------ END FUNCTION ------------------------ #

    def get_an_nft_tokens_metadata(self,
                                   contract_address: str,
                                   token_id: str,
                                   list_of_metadata_fields_to_extract: list = [],
                                   chain: str = "eth",
                                   format: str = "") -> dict:
        """
          This method gets the metadata for a specific token in a contract.
          Args:
              contract_address: the address of the NFT contract
              token_id: the id of the NFT for which to get the metadata.
              list_of_metadata_fields_to_extract: By default, all of the metadata stored at the URI returned
                by the smart contract is returned in 1 column of the dataframe in json format. If a list
                of metadata fields is provided, each of those fields is extracted from the json, and put
                into its own column. Eg. this list might look like ['name', 'description', 'image']
              chain: a string that represents the chain one is interested in. Eg, eth, ropsten, matic, etc.
              format: 'decimal' or 'hex' (decimal is default).
          Returns:
              A dictionary with the data.
        """
        api_path = f"/api/v2/nft/{contract_address}/{token_id}"

        dict_api_query_params = {
            "chain": chain
        }
        if format:
            dict_api_query_params["format"] = format

        dict_nft_metadata = self.__make_one_api_call(self.__api_network_location,
                                                     api_path,
                                                     dict_api_query_params)

        # The metadata returned in json format specified by the smart contract when calling its URI
        # function is returned all together in one field. If specified by the list_of_metadata_fields_to_extract
        # parameter of this function, the following loop will extract certain fields from the
        # single json metadata field and place them in the dictionary under their own heading.
        # Another way to think of this is that Moralis returns metadata in a dictionary, with
        # one of the fields of that dictionary being another sub-dictionary with the NFT's most interesting
        # metadata; the loop below moves the specified fields from the 'second-level' sub dictionary
        # to the top level parent dictionary. This is useful when multiple tokens are queried, because
        # if they are placed in a structure like a dataframe, each of these fields will have its own
        # column.
        dict_metadata_from_token_uri = json.loads(dict_nft_metadata['metadata'])
        for item in list_of_metadata_fields_to_extract:
            if item in dict_metadata_from_token_uri:
                dict_nft_metadata[item] = dict_metadata_from_token_uri[item]
            else:
                # Even if the field does not exist in the returned metadata json object
                # we'll add it as an empty string to the dictionary to be returned
                # for consistency.
                dict_nft_metadata[item] = None

        return dict_nft_metadata
    # ------------------------ END FUNCTION ------------------------ #

    def get_many_nft_tokens_metadata(self,
                                     contract_address: str,
                                     token_ids: Union[list, set, tuple, pd.Series],
                                     list_of_metadata_fields_to_extract: list = [],
                                     chain: str = "eth",
                                     format: str = "") -> pd.DataFrame:
        """
          This method gets the metadata for several NFT tokens.
          Args:
              contract_address: the account that one is interested in looking at the transactions for (usually
                an EOA, but not necessarily.)
              token_ids: any iterable where each item is a string representing a token id.
              list_of_metadata_fields_to_extract: By default, all of the metadata stored at the URI returned
                by the smart contract is returned in 1 column of the dataframe in json format. If a list
                of metadata fields is provided, each of those fields is extracted from the json, and put
                into its own column. Eg. this list might look like ['name', 'description', 'image']
              chain: a string that represents the chain one is interested in. Eg, eth, ropsten, matic, etc.
              format: 'decimal' or 'hex' (decimal is default).
          Returns:
              A pandas dataframe with the data.
        """
        dict_api_query_params = {
            "chain": chain
        }
        if format:
            dict_api_query_params["format"] = format

        list_of_tokens = []
        percent_tracker = PercentTracker(len(token_ids), int_output_every_x_percent=5)
        counter = 0
        for item in token_ids:
            list_of_tokens.append(
                self.get_an_nft_tokens_metadata(contract_address,
                                                item,
                                                list_of_metadata_fields_to_extract,
                                                chain,
                                                format)
            )
            counter += 1
            percent_tracker.update_progress(counter,
                                            show_time_remaining_estimate=True,
                                            str_description_to_include_in_logging="Getting a list of NFT's metadata.")
        return pd.DataFrame(list_of_tokens)
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

            size_data_set = api_response['total']
            # After the first iteration of the loop, we get our first response from the API
            # which allows us to get information like the size of the dataset, which allows
            # us to setup a percent tracker, for example.
            if is_first_loop_iteration:
                if size_data_set > 0:
                    percent_tracker = PercentTracker(api_response['total'])
                is_first_loop_iteration = False

            if size_data_set > 0:
                # add the result to the running tally
                full_data_set.extend(api_response['result'])

            # There are several ways we can tell if there are more results that need to be fetched:
            # When all the results have been returned, the list at api_response['result'] will be empty,
            # and the cursor at api_response['cursor'] will be the empty string.
            # Either can be used to determine if the loop should end. One can also use the number of items
            # per pages in conjunctions with 'offset' to do pagination, but cursors get rid of the risk of
            # missing/duplicate items when a data set might be changing.
            cursor = api_response['cursor']
            if cursor:
                dict_api_query_parameters["cursor"] = cursor
            else:
                there_are_likely_more_results = False

            if percent_tracker:
                percent_tracker.update_progress(len(full_data_set),
                                                str_description_to_include_in_logging="done retreiving data from "
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
        self.__sesh.headers.clear()
        self.__sesh.headers.update(self.__request_headers)
        self.__sesh.headers.update({"x-api-key": getenv('MORALIS_KEY')})
    # ------------------------ END FUNCTION ------------------------ #
