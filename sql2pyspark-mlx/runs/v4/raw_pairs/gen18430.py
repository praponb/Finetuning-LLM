from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount_bucket").desc(), "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w6 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("merchant").orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_id").orderBy("amount_bucket", "id")
w9 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w10 = Window.partitionBy("txn_type").orderBy("amount_avg_2_minus_running_amount", "id").rowsBetween(-3, -1)

with_windows = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(3).over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w2))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows_transactions = (
    latest
    .withColumn("amount_avg_2", F.avg("amount").over(w4))
    .withColumn("amount_bucket_2", F.ntile(4).over(w5))
    .withColumn("running_amount", F.sum("amount").over(w6))
    .withColumn("first_known_amount_bucket", F.first("amount_bucket", ignorenulls=True).over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
)

filtered = (
    with_windows_transactions
    .filter(F.col("amount_bucket_2") > 6)
    .select(
        "*",
        (F.col("amount_avg_2") - F.col("running_amount")).alias("amount_avg_2_minus_running_amount"),
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

with_windows_transactions_2 = (
    filtered
    .withColumn("amount_bucket_sum_90d", F.sum("amount_bucket").over(w9))
    .withColumn("amount_bucket_avg_3_prior", F.avg("amount_bucket").over(w10))
)

pivoted = (
    with_windows_transactions_2
    .groupBy("account_id")
    .pivot("txn_type", ["FEE", "DEBIT", "REFUND"])
    .agg(F.max("amount_bucket_2").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "account_id",
        F.col("FEE_total").alias("fee_total"),
        F.col("FEE_item_count").alias("fee_item_count"),
        F.col("DEBIT_total").alias("debit_total"),
        F.col("DEBIT_item_count").alias("debit_item_count"),
        F.col("REFUND_total").alias("refund_total"),
        F.col("REFUND_item_count").alias("refund_item_count"),
    )
)

result = (
    pivoted
    .select(
        "account_id",
        "fee_total",
        "debit_total",
        "debit_item_count",
        "refund_total",
        "refund_item_count",
    )
    .orderBy("refund_item_count")
)
