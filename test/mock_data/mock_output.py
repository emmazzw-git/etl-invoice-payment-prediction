from test.session_wrapper import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

data = [
    (
        3,
        "2024",
        "03",
        "107",
        16.67,
        "company3",
        "persion3",
        "333-333"
    ),
    (
        3,
        "2024",
        "02",
        "100, 101",
        1.56,
        "company3",
        "persion3",
        "333-333"
    ),
    (
        4,
        "2024",
        "03",
        "103, 104",
        -4.89,
        "company4",
        "persion4",
        "444-444"
    ),
    (
        5,
        "2024",
        "03",
        "105, 106",
        2.67,
        "company5",
        "persion5",
        "555-555"
    ),
    (
        5,
        "2024",
        "02",
        "102",
        12.22,
        "company5",
        "persion5",
        "555-555"
    )
]

schema = StructType([
    StructField("account_id", IntegerType()),
    StructField("year", StringType()),
    StructField("month", StringType()),
    StructField("invoice_id_list", StringType(), False),
    StructField("account_monthly_profit", DoubleType()),
    StructField("company_name", StringType()),
    StructField("contact_person", StringType()),
    StructField("contact_phone", StringType())
])

mock_output_df = spark.createDataFrame(data, schema)