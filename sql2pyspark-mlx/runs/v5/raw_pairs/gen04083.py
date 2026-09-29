from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "session_start_flag",
        "island_id",
        F.when(F.col("merchant").rlike("^(Amazon|Shell)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("merchant_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("merchant").desc_nulls_last())
)
