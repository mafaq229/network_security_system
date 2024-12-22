import sys
from networksecurity.logging import logger


class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_details: sys):
        self.error_message = error_message
        _, _, exe_tb = error_details.exc_info()
        
        self.linenum = exe_tb.tb_lineno
        self.filename = exe_tb.tb_frame.f_code.co_filename
        
    def __str__(self):
        return "Error occured in python script name [{0}], line number [{1}] with error message [{2}]".format(
            self.filename, self.linenum, str(self.error_message))


if __name__ == "__main__":
    try:
        logger.logging.info("Enter the try block")
        a = 1/0
        print("this will not be printed", a)
    except Exception as e:
        raise NetworkSecurityException(e, sys)
