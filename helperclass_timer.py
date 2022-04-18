import logging
import time


class Timer:
    """
        This class provides several 'timing' capabilities. It is initially being coded to assist
        situations where APIs have rate limits, eg. only 5 requests per second. This class
        will abstract the functionality of 'waiting' to make the next api call within the rate
        limits, but I suspect it will grow to have other functionality as well.
    """

    def __init__(self, rate_limit: int = 5):
        """
        Initialize an instance of the class.
          Args:
              rate_limit: APIs usually have a rate limit given in number of calls that can be made
              per second. So, for example, if an API says in its documentation that its limit is
              5 calls per second, then the integer 5 should be passed to this constructor.
        """
        self.start_time = 0
        # As a decimal, a rate limit is 1 / rate_limit
        # For example, if an api's limit is 5 calls per second, this means that a call can be made
        # every 0.2 seconds max ( 1 / 5 )
        self.__limit_as_decimal = 1 / rate_limit
    # ------------------------ END FUNCTION ------------------------ #

    def reset(self):
        """
          This method restarts the timer.
        """
        self.start_time = time.time()
    # ------------------------ END FUNCTION ------------------------ #

    def wait_until_allowed(self, include_reset: bool = False):
        """
          This method pauses code execution until the code should be allowed
          to continue, based on the rate_limit given to the class constructor.
          Args:
              include_reset: If this is set to true, then this method will also
              reset the timer, so that an implicit call to the 'reset' method
              is not needed.
        """
        ellapsed_time = time.time() - self.start_time
        if ellapsed_time < self.__limit_as_decimal:
            logging.debug("Delaying api call in 'wait_until_allowed' method.")
            time.sleep(self.__limit_as_decimal - ellapsed_time)
        if include_reset:
            self.reset()
    # ------------------------ END FUNCTION ------------------------ #
