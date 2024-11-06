from unittest.mock import MagicMock
from app.transformer.profit_calculator import ProfitCalculator
from app.lib.logger.logging_service import LoggingService
from test.mock_data.account_df_mock import mock_account_df
from test.mock_data.invoice_line_items_df_mock import mock_invoice_line_items_df
from test.mock_data.invoices_df_mock import mock_invoices_df
from test.mock_data.skus_df_mock import mock_skus_df
from test.mock_data.mock_output import mock_output_df
from chispa.dataframe_comparer import assert_df_equality
from test.session_wrapper import spark


def test_profit_calculator():
    logger_mock = MagicMock()
    test_transformer = ProfitCalculator(
        logger_mock,
        mock_account_df,
        mock_invoice_line_items_df,
        mock_invoices_df,
        mock_skus_df
    )

    logger_real = LoggingService().get_logger(spark)
    df_transformed = test_transformer.calculate_profit()
    logger_real.info(f"df_transformed.show()==={df_transformed.show()}")
    
    df_expected = mock_output_df

    assert_df_equality(df_transformed, df_expected)