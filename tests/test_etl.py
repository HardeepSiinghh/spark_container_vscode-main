import pytest
from chispa.dataframe_comparer import assert_df_equality

from src.etl import get_spark_session, process_revenue


@pytest.fixture(scope="session")
def spark():
    return get_spark_session()


def test_process_revenue_logic(spark):
    # 1. Arrange: Create input data
    source_data = [
        ("Client_A", "active", 100.0),
        ("Client_B", "inactive", 50.0),
        ("Client_C", "active", 200.0),
    ]
    source_df = spark.createDataFrame(source_data, ["client", "status", "amount"])

    # 2. Act: Run the transformation
    actual_df = process_revenue(source_df)

    # 3. Assert: Define expected output
    expected_data = [
        ("Client_A", "active", 100.0, 120.0),
        ("Client_C", "active", 200.0, 240.0),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["client", "status", "amount", "total_revenue"]
    )

    # Chispa provides detailed error messages if dataframes mismatch
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
