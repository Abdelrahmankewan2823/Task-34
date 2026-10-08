from pyspark.sql import functions as F


def clean_data(df):
    # keep only rows with a positive amount
    df = df.filter(F.col("amount") > 0)

    # drop rows where name is null
    df = df.filter(F.col("name").isNotNull())

    # add the amount with 20% tax
    df = df.withColumn("amount_with_tax", F.col("amount") * 1.20)

    return df
