from test.session_wrapper import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    (
        100,
        "item1",
        3
    ),
    (
        100,
        "item2",
        2
    ),
    (
        100,
        "item3",
        1
    ),
    (
        101,
        "item4",
        4
    ),
    (
        101,
        "item3",
        5
    ),
    (
        102,
        "item5",
        3
    ),
    (
        102,
        "item1",
        2
    ),
    (
        103,
        "item4",
        1
    ),
    (
        104,
        "item2",
        3
    ),
    (
        104,
        "item3",
        2
    ),
    (
        105,
        "item1",
        3
    ),
    (
        106,
        "item2",
        2
    ),
    (
        106,
        "item3",
        3
    ),
    (
        107,
        "item5",
        5
    )
]

schema = StructType([
    StructField("invoice_id", IntegerType()),
    StructField("item_id", StringType()),
    StructField("quantity", IntegerType())
])

mock_invoice_line_items_df = spark.createDataFrame(data, schema)