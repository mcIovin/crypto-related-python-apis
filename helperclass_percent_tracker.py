import logging
import time


class PercentTracker:
    """This class can assist in displaying percentages of functions
    that have loops."""

    def __init__(self, max_value_of_loop, int_output_every_x_percent=10,
                 console=False, log=True, log_level=logging.INFO):
        """Initialize a percent tracking pandas_friendly_object."""
        self.max_loop_number = max_value_of_loop
        self.float_percent_counter = 0
        self.previous_time = 0
        self.start_time = time.time()
        self.float_print_every_x_percent = int_output_every_x_percent / 100
        self.log_level = log_level
        self.print_to_log = log
        self.print_to_console = console

    # ------------------------ END FUNCTION ------------------------ #

    def update_progress(self, counter, show_time_remaining_estimate=False,  # noqa: C901
                        str_description_to_include_in_logging=''):
        """Update the percent tracker with the latest counter new_value."""
        float_progress = counter / self.max_loop_number
        if float_progress > self.float_percent_counter:
            now_time = time.time()
            time_elapsed = now_time - self.start_time
            estimate_total_time = (time_elapsed * self.max_loop_number) / counter
            estimate_time_remaining = estimate_total_time - time_elapsed
            self.previous_time = now_time

            list_messages = [
                'Time elapsed is ' + "{:.4f}".format(time_elapsed) + ' seconds.',
                'Percent complete: ' + "{:.2%}".format(float_progress) + ' - ' + str_description_to_include_in_logging
            ]

            for msg in list_messages:
                if self.print_to_log:
                    logging.log(self.log_level, msg)
                if self.print_to_console:
                    print(msg)
            if show_time_remaining_estimate:
                # the message below can't be included in the list (and loop) of messages above, because
                # before displaying it, we need to check that the percent counter
                # isn't still zero. If it is, then a time estimate cannot yet be provided.
                if self.float_percent_counter != 0:
                    if self.print_to_log:
                        if estimate_time_remaining < 60:
                            logging.log(self.log_level,
                                        "Estimated time remaining is " +
                                        "{:.1f}".format(estimate_time_remaining) + " seconds.")
                        else:
                            logging.log(self.log_level,
                                        "Estimated time remaining is " + str(int(estimate_time_remaining / 60)) +
                                        " minutes.")
                    if self.print_to_console:
                        if estimate_time_remaining < 60:
                            print(
                                'Estimated time remaining is ' + "{:.1f}".format(
                                    estimate_time_remaining) + ' seconds.')
                        else:
                            print(
                                'Estimated time remaining is ' + str(
                                    int(estimate_time_remaining / 60)) + ' minutes.')

            self.float_percent_counter += self.float_print_every_x_percent
    # ------------------------ END FUNCTION ------------------------ #