# This schema file could be refactorer by a metadata driven data solution 
# to transform the schema from yml config
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

accounts_schema = StructType() \
    .add("account_id", IntegerType()) \
    .add("company_name", StringType()) \
    .add("company_address", StringType()) \
    .add("contact_person", StringType()) \
    .add("contact_phone", StringType()) \
    .add("gender", StringType()) \
    .add("joining_date", StringType())

invoice_line_items_schema = StructType() \
    .add("invoice_id", IntegerType()) \
    .add("item_id", StringType()) \
    .add("quantity", IntegerType())

invoices_schema = StructType() \
    .add("invoice_id", IntegerType()) \
    .add("account_id", IntegerType()) \
    .add("date_issued", StringType())

skus_schema = StructType() \
    .add("item_id", StringType()) \
    .add("item_name", StringType()) \
    .add("item_description", StringType()) \
    .add("item_cost_price", DoubleType()) \
    .add("item_retail_price", DoubleType())
