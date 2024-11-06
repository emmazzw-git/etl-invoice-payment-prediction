from dataclasses import dataclass
from pyspark.sql import DataFrame

@dataclass
class FeatureWriter:
    """
    Feature Writer writes the invoice prediction feature dataframe to path
    """

    invoice_prediction_df: DataFrame
    output_path: str

    def write_feature_to_dest(self):

        self.invoice_prediction_df \
            .write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .csv(self.output_path)


