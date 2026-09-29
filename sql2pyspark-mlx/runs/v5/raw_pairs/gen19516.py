from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-10-14 00:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        "txn_type_upper",
        "resolved_merchant",
    )
)
