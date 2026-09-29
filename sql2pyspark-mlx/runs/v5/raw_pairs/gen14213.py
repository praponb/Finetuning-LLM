from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("merchant_length").desc(), "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-08-21").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
