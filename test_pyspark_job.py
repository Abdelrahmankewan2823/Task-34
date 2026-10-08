import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("clean-data-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_df(spark, rows):
    return spark.createDataFrame(rows, ["id", "name", "amount"])


def test_valid_records_are_kept(spark):
    df = make_df(spark, [(1, "Ahmed", 50.0), (2, "Sara", 200.0)])
    result = clean_data(df)
    assert result.count() == 2


def test_amount_less_than_or_equal_zero_removed(spark):
    df = make_df(spark, [
        (1, "Ahmed", 50.0),
        (2, "Sara", 0.0),
        (3, "Omar", -10.0),
    ])
    result = clean_data(df)
    names = [row["name"] for row in result.collect()]
    assert names == ["Ahmed"]


def test_null_names_removed(spark):
    df = make_df(spark, [
        (1, "Ahmed", 50.0),
        (2, None, 80.0),
    ])
    result = clean_data(df)
    assert result.count() == 1
    assert result.collect()[0]["name"] == "Ahmed"


def test_amount_with_tax_is_correct(spark):
    df = make_df(spark, [(1, "Ahmed", 100.0), (2, "Sara", 50.0)])
    result = clean_data(df)
    values = {row["name"]: row["amount_with_tax"] for row in result.collect()}
    assert values["Ahmed"] == pytest.approx(120.0)
    assert values["Sara"] == pytest.approx(60.0)
