from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("running_max_amount", "id").rowsBetween(-2, -1)
w4 = Window.orderBy(F.col("running_sum_amount").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id", "merchant").orderBy(F.col("running_max_amount").desc())
w_part = Window.partitionBy("account_id", "merchant").orderBy(F.col("running_sum_amount").desc(), "id")
w9 = Window.partitionBy("account_id")
w10 = Window.partitionBy("account_id").orderBy(F.col("row_seq").desc())
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

filtered = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Starbucks")))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("txn_type").isin(["FEE", "REFUND"]), "Core").otherwise("Other").alias("txn_type_group"),
        F.date_trunc("week", "txn_time").alias("txn_time_period"),
    )
)

filtered_transactions = (
    filtered
    .filter(F.col("amount") > 3600)
    .withColumn("txn_time_label", F.date_format("txn_time", "yyyy-MM-dd"))
)

metrics = (
    filtered_transactions
    .filter(F.col("account_id").isin([5, 34]))
    .withColumn("running_avg_amount", F.avg("amount").over(w_ordered))
    .withColumn("running_sum_amount", F.sum("amount").over(w_ordered))
    .withColumn("running_max_amount", F.max("amount").over(w_ordered))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w2))
)

scored = (
    metrics
    .withColumn("first_known_amount_avg_2_prior", F.avg("first_known_amount").over(w_running))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("cumulative_running_sum_amount", F.sum("running_sum_amount").over(w5))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w6))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.ntile(4).over(w_part).alias("running_sum_amount_bucket"),
        F.max("first_known_amount").over(w9).alias("account_id_max_first_known_amount"),
        F.cume_dist().over(w10).alias("row_seq_cume_dist"),
    )
)

result = (
    with_windows
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_type_group",
        "txn_time_period",
        "running_max_amount",
        F.col("avg_transaction_amount"),
    )
    .orderBy("txn_time")
)
