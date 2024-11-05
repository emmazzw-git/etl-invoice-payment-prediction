import sys
from pyspark.sql import SparkSession
from dataclasses import dataclass
from lib.logger.logging_service import LoggingService
from reader.csv_reader import CsvReader


@dataclass
class InvoicePredictionEtlJob(LoggingService):
    stage: str
    app_name: str

    def run(self, input_csv: str):
      self.spark =  SparkSession \
                    .builder \
                    .master(self.stage) \
                    .appName(self.app_name) \
                    .getOrCreate()
      self.logger = self.get_logger(self.spark)

      ########################################################
      ## 1. Set up the CSV Reader
      ########################################################
      reader = CsvReader(self.spark, self.logger, input_csv)
      self.logger.info("CSV Reader is up")
      #########################################################
      ## 2. Parse the input csv string
      ##########################################################
      reader.parse_csv_list()
      self.logger.info("CSV file list is parsed")
      #########################################################
      
      ## TODO Validate the file names
      ## If the DBA sends the files to the landing zone,
      ## File names normally are with the file date
      ## Need to check if file names are following agreed patterns

      #########################################################
      ## 3. Load source files with schemas
      ##########################################################
      accounts_df_raw, invoice_line_items_df_raw, invoices_df_raw, skus_df_raw = reader.load_raw_file_with_schema()
      self.logger.info("Raw file loaded with schema")

      ## TODO Process the bad records either save in tables or retry

      #########################################################
      ## 4. Load source files with schemas
      ##########################################################
      self.logger.info(accounts_df_raw.show(3))
      self.logger.info(invoice_line_items_df_raw.show(2))
      self.logger.info(invoices_df_raw.show(2))
      self.logger.info(skus_df_raw.show(2))
 

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
