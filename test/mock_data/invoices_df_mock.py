from test.session_wrapper import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    (
        100,
        3,
        "2024-02-01"
    ),
    (
        101,
        3,
        "2024-02-02"
    ),
    (
        102,
        5,
        "2024-02-03"
    ),
    (
        103,
        4,
        "2024-03-01"
    ),
    (
        104,
        4,
        "2024-03-02"
    ),
    (
        105,
        5,
        "2024-03-01"
    ),
    (
        106,
        5,
        "2024-03-02"
    ),
    (
        107,
        3,
        "2024-03-02"
    )
]

schema = StructType([
    StructField("invoice_id", IntegerType()),
    StructField("account_id", IntegerType()),
    StructField("date_issued", StringType())
])

mock_invoices_df = spark.createDataFrame(data, schema)