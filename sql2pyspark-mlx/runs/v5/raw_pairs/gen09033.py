from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("merchant", "account_id").orderBy("txn_time_hour", "id")
w5 = Window.orderBy("txn_time_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy("is_new_burst", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-05-27"))) & F.col("is_flagged"))
    .withColumn("txn_time_hour", F.hour("txn_time"))
)

cleaned_transactions = (
    cleaned
    .filter(F.col("txn_time") >= F.lit("2023-08-04").cast("date"))
    .withColumn("txn_time_hour_2", F.hour("txn_time"))
)

gaps = (
    cleaned_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

metrics = (
    islands
    .filter(F.col("is_new_burst") == 1)
    .withColumn("last_known_txn_time_hour", F.last("txn_time_hour", ignorenulls=True).over(w2))
    .withColumn("running_amount", F.sum("amount").over(w_ordered))
    .withColumn("txn_time_hour_bucket", F.ntile(5).over(w_running_2))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w5))
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_new_burst",
        "burst_id",
        "last_known_txn_time_hour",
        "running_amount",
        "last_known_amount",
    )
)

result = (
    deduped
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_time",
        "burst_id",
        "last_known_txn_time_hour",
        "running_amount",
        "last_known_amount",
        F.col("min_transaction_amount"),
    )
    .orderBy("running_amount")
)
