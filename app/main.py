import sys
from pyspark.sql import SparkSession
from lib.logger.logging_service import LoggingService


class InvoicePredictionEtlJob(LoggingService):
    def __init__(self, stage, app_name):
      self.spark =  SparkSession \
                      .builder \
                      .master(stage) \
                      .appName(app_name) \
                      .getOrCreate()
      self.logger = self.get_logger(self.spark)

    def run(self, input_csv: str):
      self.logger.info(f"+++input_csv+++==={input_csv}")

if __name__ == "__main__":
  args = sys.argv
  print(f'args are {args}')

  if len(args) < 4 and None in args:
    sys.exit("You need to submit the app with all the required params")

  stage = args[1]
  app_name = args[2]
  input_csv = args[3]
  
  job = InvoicePredictionEtlJob(stage, app_name)
  job.run(input_csv)
