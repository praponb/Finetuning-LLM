from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "burst_id",
        F.when(F.col("merchant").rlike("^(7-Eleven|Amazon)$"), "GROUP_B").when(F.col("merchant").rlike("^(Grab|Lazada)$"), "EXTENDED").otherwise("PRIORITY").alias("merchant_zone"),
        F.col("max_transaction_amount"),
    )
)
