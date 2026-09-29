from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")

base = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "txn_type", "txn_time_hour")
)
