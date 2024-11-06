from test.session_wrapper import spark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

data = [
    (
        "item1",
        "item1 name",
        "item1 description",
        1.111111,
        2.222222
    ),
    (
        "item2",
        "item2 name",
        "item2 description",
        2.22222,
        0.22222
    ),
    (
        "item3",
        "item3 name",
        "item3 description",
        3.3333,
        4.4444
    ),
    (
        "item4",
        "item4 name",
        "item4 description",
        4.44444444,
        3.33333333
    ),
    (
        "item5",
        "item5 name",
        "item5 description",
        5.555555,
        8.888888
    )
]

schema = StructType([
    StructField("item_id", StringType()),
    StructField("item_name", StringType()),
    StructField("item_description", StringType()),
    StructField("item_cost_price", DoubleType()),
    StructField("item_retail_price", DoubleType())
])

mock_skus_df = spark.createDataFrame(data, schema)