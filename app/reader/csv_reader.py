import ast
import logging
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple
from reader.schema import accounts_schema, invoice_line_items_schema, invoices_schema, skus_schema


@dataclass
class CsvReader:
    """
    CSV Reader reads a list of CSV files, validates the schema and data,
    checks bad records and saves the data in spark dataframes
    """

    spark: SparkSession
    logger: logging.Logger
    input_csv: str

    def parse_csv_list(self):
        self.csv_list = ast.literal_eval(self.input_csv)

    def load_raw_file(self, source_file_schema, source_file_lineSep, fpath):
        return (
            self.spark.read.option("delimiter", ",")
            .option("header", True)
            .option("lineSep", source_file_lineSep)
            .option("badRecordsPath", "/opt/spark-app/bad_records/source_file_schema.json") # refactor the bad records file path with more customised names
            .schema(source_file_schema)
            .csv(
                path=fpath,
                maxColumns=10000000,
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
                dateFormat="ddMMyyyy"
            )
        ).cache()
    
    def load_raw_file_with_schema(self) -> Tuple[DataFrame]:
        self.accounts_df_raw = self.spark.createDataFrame([], accounts_schema)
        self.invoice_line_items_df_raw = self.spark.createDataFrame([], invoice_line_items_schema)
        self.invoices_df_raw = self.spark.createDataFrame([], invoices_schema)
        self.skus_df_raw = self.spark.createDataFrame([], skus_schema)

        for fpath in self.csv_list:
            fname = fpath.split("/")[-1]
            if fname == "accounts.csv":
                self.accounts_df_raw = self.load_raw_file(accounts_schema, "\r", fpath)
            elif fname == "invoice_line_items.csv":
                self.invoice_line_items_df_raw = self.load_raw_file(invoice_line_items_schema, "\n", fpath)
            elif fname == "invoices.csv":
                self.invoices_df_raw = self.load_raw_file(invoices_schema, "\n", fpath)
            elif fname == "skus.csv":
                self.skus_df_raw = self.load_raw_file(skus_schema, "\n", fpath)
            else:
                raise NameError("Wrong file name")
            
        # Trigger bad records generation from the read with schema
        try:
            self.accounts_df_raw.write.mode("overwrite").format("noop").save()
            self.invoice_line_items_df_raw.write.mode("overwrite").format("noop").save()
            self.invoices_df_raw.write.mode("overwrite").format("noop").save()
            self.skus_df_raw.write.mode("overwrite").format("noop").save()
        except Exception as e:
            self.logger.error(f"{fpath}: Data type mis-matches detected in source files")
            self.logger.error(str(e))

        return self.accounts_df_raw, self.invoice_line_items_df_raw, self.invoices_df_raw, self.skus_df_raw
