from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "merchant",
        "is_flagged",
        "is_new_burst",
        "island_id",
        F.col("sum_transaction_amount"),
    )
    .orderBy("id")
    .limit(5)
)
