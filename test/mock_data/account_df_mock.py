from test.session_wrapper import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [
    (
        3,
        "company3",
        "company address 3",
        "persion3",
        "333-333",
        "M",
        "01/11/2024"
    ),
    (
        4,
        "company4",
        "company address 4",
        "persion4",
        "444-444",
        "M",
        "02/11/2024"
    ),
    (
        5,
        "company5",
        "company address 5",
        "persion5",
        "555-555",
        "F",
        "03/11/2024"
    )
]

schema = StructType([
    StructField("account_id", IntegerType()),
    StructField("company_name", StringType()),
    StructField("company_address", StringType()),
    StructField("contact_person", StringType()),
    StructField("contact_phone", StringType()),
    StructField("gender", StringType()),
    StructField("joining_date", StringType())
])

mock_account_df = spark.createDataFrame(data, schema)