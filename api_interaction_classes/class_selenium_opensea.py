import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class SeleniumOnOpensea:
    """This class assists with 'browsing' OpenSea using Seleium."""

    def __init__(self):
        """Initialize an object for RV website Authentication."""

        self.driver = webdriver.Firefox
    # ------------------------ END FUNCTION ------------------------ #

    def start_driver(self):
        # initialize the Selenium webdriver for Firefox, which literally
        # opens a browser
        logging.info('Starting Selenium browser driver.')
        try:
            self.driver = webdriver.Firefox()
        except Exception as e:
            logging.error(f"Unable to start Selenium browser driver. The exception was: "
                          f"{repr(e)}")
    # ------------------------ END FUNCTION ------------------------ #

    def login_rv_website(self):
        """Login to the RV website."""
        # tell the driver to browse to the RV website so we can proceed to authenticate
        try:
            self.driver.get('https://www.realvision.com/rv/login')
            # fill out the username
            username_area = WebDriverWait(self.driver, timeout=30). \
                until(EC.presence_of_element_located((By.ID, "email")))
            username_area.send_keys("somename")
            # fill out the password
            pwd_area = WebDriverWait(self.driver, timeout=30). \
                until(EC.presence_of_element_located((By.ID, "password")))
            pwd_area.send_keys("somepassword")
            logging.info('Successfully logged in to RV website.')
        except Exception as e:
            logging.warning(f"Problem during function 'login_rv_website'. "
                            f"The Exception was: {repr(e)}")
    # ------------------------ END FUNCTION ------------------------ #

    def close_driver(self):
        try:
            self.driver.close()
        except Exception as e:
            logging.error(f"Unable to close Selenium browser driver. The exception was: {repr(e)}")
        logging.info('Closed browser.')
    # ------------------------ END FUNCTION ------------------------ #
