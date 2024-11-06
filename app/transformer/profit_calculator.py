import logging
from dataclasses import dataclass
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

@dataclass
class ProfitCalculator:
    """
    Transformer that calculates the total amount payable for each account on each invoice issued date
    """

    logger: logging.Logger
    accounts_df_raw: DataFrame
    invoice_line_items_df_raw: DataFrame
    invoices_df_raw: DataFrame
    skus_df_raw: DataFrame


    def calculate_profit(self) -> DataFrame:
        # Calculate the item net profit
        self.skus_df = self.skus_df_raw.withColumn(
            "item_net_profit",
            F.coalesce((F.col("item_retail_price") - F.col("item_cost_price")), F.lit(0))
        )

        # Join the dataframes to include the item net profit in line item df
        self.invoice_item_total_df = self.invoice_line_items_df_raw.join(
            self.skus_df,
            how="left",
            on=(self.invoice_line_items_df_raw["item_id"] == self.skus_df["item_id"])
        ).select("invoice_id", self.invoice_line_items_df_raw.item_id, "quantity", "item_net_profit")

        # If the item is not found in skus table, set net profit as 0
        self.invoice_item_total_df = self.invoice_item_total_df.withColumn(
            "item_net_profit",
            F.coalesce(F.col("item_net_profit"), F.lit(0))
        )

        # Calculate the item total profit
        self.invoice_item_total_df = self.invoice_item_total_df.withColumn(
            "item_total_profit",
            (F.col("item_net_profit") * F.col("quantity"))
        )

        self.logger.info(self.invoice_item_total_df.show(3))

        # Aggregate the total profit for each invoice
        self.invoice_total_df = self.invoice_item_total_df.groupBy(
            "invoice_id"
        ).agg(
            F.sum("item_total_profit").alias("invoice_profit")
        )
        
        # Join the dataframes to include the invoice profit in the invoices df
        self.invoices_df = self.invoices_df_raw.join(
            self.invoice_total_df,
            how="left",
            on=(self.invoices_df_raw["invoice_id"] == self.invoice_total_df["invoice_id"])
        ).select(
            self.invoices_df_raw.invoice_id, "account_id", "date_issued", "invoice_profit"
        )

        # Aggregate the total profit for each account and all invoice ids for each account and date_issued
        self.account_profit_df = self.invoices_df.groupBy(
            "account_id",
            "date_issued"
        ).agg(
            F.collect_list(self.invoices_df.invoice_id).alias("invoice_id_list"),
            F.sum("invoice_profit").alias("account_daily_profit")
        )

        # Add two columns year and month for aggregate account profit by year/month
        self.account_profit_by_month_df = self.account_profit_df.withColumn(
            "year",
            F.split(self.account_profit_df["date_issued"], "-").getItem(0)
        ).withColumn(
            "month",
            F.split(self.account_profit_df["date_issued"], "-").getItem(1)
        )

        # Aggregate account profit by month and list out the invoice ids
        self.account_monthly_profit_df = self.account_profit_by_month_df.groupBy(
            "account_id",
            "year",
            "month"
        ).agg(
            F.concat_ws(
                ", ",
                F.array_sort(
                    F.flatten(
                        F.collect_list(self.account_profit_by_month_df.invoice_id_list)
                    )
                )
            ).alias("invoice_id_list"),
            F.round(F.sum("account_daily_profit"), 2).alias("account_monthly_profit")
        )

        # Join the dataframes to include the account info to the account monthly profit df
        self.invoice_prediction_df = self.account_monthly_profit_df.join(
            self.accounts_df_raw,
            how="left",
            on=(self.account_monthly_profit_df["account_id"] == self.accounts_df_raw["account_id"])
        ).select(
            self.account_monthly_profit_df.account_id,
            "year",
            "month",
            "invoice_id_list",
            "account_monthly_profit",
            "company_name",
            "contact_person",
            "contact_phone"
        ).orderBy(
            "account_id",
            F.desc("year"),
            F.desc("month")
        )

        return self.invoice_prediction_df
