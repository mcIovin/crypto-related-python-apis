import logging
import pandas as pd
from urllib.parse import urlunsplit, urlencode
from typing import Union
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webelement import WebElement
from helperclass_percent_tracker import PercentTracker


class SeleniumOnOpensea:
    """This class assists with 'browsing' OpenSea using Seleium."""

    # URL components
    __website_scheme = 'https'
    __website_network_location = 'opensea.io'

    def __init__(self):
        """Initialize an object for RV website Authentication."""
        self.driver = webdriver.Firefox
    # ------------------------ END FUNCTION ------------------------ #

    def start_driver(self, headless: bool = True):
        # initialize the Selenium webdriver for Firefox, which literally
        # opens a browser when running in non-headless mode.
        logging.info('Starting Selenium browser driver.')

        if headless:
            options = Options()
            options.headless = True

        try:
            if headless:
                self.driver = webdriver.Firefox(options=options)
            else:
                self.driver = webdriver.Firefox()
        except Exception as e:
            logging.error(f"Unable to start Selenium browser driver. The exception was: "
                          f"{repr(e)}")
    # ------------------------ END FUNCTION ------------------------ #

    def get_many_nfts_properties(self,
                                 contract_address: str,
                                 token_ids: Union[list, set, tuple, pd.Series],
                                 include_numeric_traits: bool = True) -> pd.DataFrame:

        # We'll make a copy of the iterable (with the tokens), to keep track of which ones
        # have been successfully processed.
        list_for_success_tracking = list(token_ids)

        list_results = []

        num_tokens = len(token_ids)
        percent_tracker = PercentTracker(num_tokens,
                                         int_output_every_x_percent=5)
        counter = 0
        attempt_number = 1
        # often there might be errors when getting a particular token, so the outer (while) loop
        # below will attempt several times to get the data.
        while list_for_success_tracking and attempt_number <= 3:
            logging.info(f"Starting attempt #{attempt_number} to get the NFT properties.")

            for item in token_ids:
                token_processing_successful = True
                try:
                    list_results.append(self.get_an_nfts_properties(contract_address, item, include_numeric_traits))
                except Exception as e:
                    logging.error(f"Unable to get metadata properties for token {item}. The exception was: "
                                  f"{repr(e)}")
                    token_processing_successful = False

                if token_processing_successful:
                    # If the processing was successful, we remove it from the list
                    # that we are using to track this.
                    list_for_success_tracking.remove(item)
                    counter += 1

                percent_tracker.update_progress(counter,
                                                show_time_remaining_estimate=True,
                                                str_description_to_include_in_logging="Getting properties for a list of "
                                                                                      "NFTs.")

            # Reset the list (that the for loop uses) to only the remaining tokens that had errors,
            # so the method can try to get the dat for those tokens again.
            # The list is deliberately cast from a list to a list in order to create a deep copy.
            token_ids = list(list_for_success_tracking)

            attempt_number += 1

        return pd.DataFrame(list_results)
    # ------------------------ END FUNCTION ------------------------ #

    def get_an_nfts_properties(self,
                               contract_address: str,
                               token_id: str,
                               include_numeric_traits: bool = True) -> Union[None, dict]:
        dict_with_nft_properties = {}
        dict_with_nft_properties['token_id'] = token_id
        opensea_url = self.__construct_opensea_url_for_a_specific_nft(contract_address, token_id)
        if self.__load_opensea_page(opensea_url) == False:
            return

        div_with_nft_data = self.__get_div_with_nft_summary()
        dict_with_nft_properties.update(self.__extract_nft_properties_from_summary_div(div_with_nft_data))

        if include_numeric_traits:
            dict_nft_numeric_traits = self.__extract_nft_numeric_traits_from_summary_div(div_with_nft_data)
            dict_with_nft_properties.update(dict_nft_numeric_traits)

        return dict_with_nft_properties
    # ------------------------ END FUNCTION ------------------------ #

    def close_driver(self):
        try:
            self.driver.close()
        except Exception as e:
            logging.error(f"Unable to close Selenium browser driver. The exception was: {repr(e)}")
        logging.info('Closed browser.')
    # ------------------------ END FUNCTION ------------------------ #

    def __construct_opensea_url_for_a_specific_nft(
            self,
            contract_address: str,
            token_id: str) -> str:

        url_path = f"/assets/{contract_address}/{token_id}"
        dict_query_params = {}

        # the urlunsplit method expects five arguments. Below, they are self-explanatory, except for the
        # fifth argument. In the fifth argument, a 'fragment' of a URL (I believe it is a portion that goes after
        # a '#' hash symbol) can be passed. We don't need that, so we just pass the empty string. I'm not sure
        # why a double parentheses is needed to call the function.
        return urlunsplit((self.__website_scheme,
                           self.__website_network_location,
                           url_path,
                           urlencode(dict_query_params),
                           ""))
    # ------------------------ END FUNCTION ------------------------ #

    def __load_opensea_page(self, opensea_url: str) -> bool:
        successfully_loaded = True
        try:
            self.driver.get(opensea_url)
        except Exception as e:
            logging.warning(f"Problem while opening: {opensea_url}. The Exception was: {repr(e)}")
            successfully_loaded = False

        return successfully_loaded
    # ------------------------ END FUNCTION ------------------------ #

    def __get_div_with_nft_summary(self) -> Union[WebElement, None]:
        """
        This function expects a webpage with information for a specific NFT to already be
        loaded by the selenium driver.
        :return: A representation of the <div> html element that contains the
          NFT's summary information.
        """

        div_with_nft_data_summary = WebElement
        try:
            div_with_nft_data_summary = WebDriverWait(self.driver, timeout=30). \
                until(EC.presence_of_element_located((By.CLASS_NAME, "item--summary")))
        except Exception as e:
            logging.warning(f"Problem while accessing the 'div' containing NFT properties. "
                            f"The Exception was: {repr(e)}")
            return

        return div_with_nft_data_summary
    # ------------------------ END FUNCTION ------------------------ #

    def __extract_nft_properties_from_summary_div(self,
                                                  div_containing_summary_of_nft_data: WebElement) -> dict:
        """
        OpenSea has the data about an NFT in a div called 'item--sumary'. This method
        expects other code to find that <div> and pass it to this method as a parameter
        :param div_containing_summary_of_nft_data:
        :return: A dict containing each of the NFT's properties and its associated value.
        """

        list_property_names = WebElement
        list_property_values = WebElement

        try:
            list_property_names = \
                div_containing_summary_of_nft_data.find_elements(by=By.CLASS_NAME, value="Property--type")
            list_property_values = \
                div_containing_summary_of_nft_data.find_elements(by=By.CLASS_NAME, value="Property--value")
        except Exception as e:
            logging.warning(f"Problem while finding the HTML elements representing the NFT's properties, "
                            f"and their respective values. The Exception was: {repr(e)}")

        return self.__make_dict_from_list_of_types_and_list_of_their_values(list_property_names, list_property_values)
    # ------------------------ END FUNCTION ------------------------ #

    def __extract_nft_numeric_traits_from_summary_div(self,
                                                      div_containing_summary_of_nft_data: WebElement) -> dict:
        """
        OpenSea has the data about an NFT in a div called 'item--sumary'. This method
        expects other code to find that <div> and pass it to this method as a parameter
        :param div_containing_summary_of_nft_data:
        :return: A dict containing each of the NFT's numeric traits and its associated value.
        """

        list_numeric_types_names = WebElement
        list_numeric_types_values = WebElement

        # As of this writing, when an OpenSea page loads, the numerical traits are 'collapsed'
        # so we need to click on a button in order to expand them before they are accessible
        # by selenium.
        list_of_button_web_elements = div_containing_summary_of_nft_data.find_elements(By.TAG_NAME, value="button")
        stats_traits_button = list_of_button_web_elements[4]
        stats_traits_button.click()

        try:
            list_numeric_types_names = \
                div_containing_summary_of_nft_data.find_elements(by=By.CLASS_NAME, value="NumericTrait--type")
            list_numeric_types_values = \
                div_containing_summary_of_nft_data.find_elements(by=By.CLASS_NAME, value="NumericTrait--value")
        except Exception as e:
            logging.warning(f"Problem while finding the HTML elements representing the NFT's numeric traits, "
                            f"and their respective values. The Exception was: {repr(e)}")

        return self.__make_dict_from_list_of_types_and_list_of_their_values(list_numeric_types_names,
                                                                            list_numeric_types_values)
    # ------------------------ END FUNCTION ------------------------ #

    @staticmethod
    def __make_dict_from_list_of_types_and_list_of_their_values(
            list_property_types: list[WebElement],
            list_property_values: list[WebElement]) -> dict:

        dict_properties = {}
        num_properties = len(list_property_types)
        num_property_values = len(list_property_values)
        if num_properties == num_property_values:
            counter = 0
            while counter < num_properties:
                current_property = list_property_types[counter].text
                current_property_value = list_property_values[counter].text
                dict_properties[current_property] = current_property_value
                counter += 1
        else:
            logging.warning("Got a different number of 'property names' from 'property values'.")

        return dict_properties
    # ------------------------ END FUNCTION ------------------------ #

