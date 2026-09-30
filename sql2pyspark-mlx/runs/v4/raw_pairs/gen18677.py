from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc_nulls_first(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2024-02-22 06:00:00")))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("merchant").alias("merchant_upper"),
    )
)

valid = (
    cleaned
    .filter((F.col("txn_time") > F.to_date(F.lit("2024-02-29"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_upper",
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
        F.upper("merchant").alias("merchant_upper_2"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_upper",
        "merchant_filled",
        "merchant_upper_2",
    )
    .orderBy(F.col("merchant").desc())
)
