from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id")
w3 = Window.orderBy(F.col("txn_time").desc(), "id")

with_windows = (
    df_transactions
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_bucket", F.ntile(10).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    with_windows
    .groupBy(F.date_trunc("month", "txn_time").alias("activity_month"))
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.min("txn_time").alias("started_at"),
        F.avg("running_amount").alias("avg_running_amount"),
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
        F.percentile_approx("amount_bucket", [0.25, 0.5, 0.75]).alias("amount_bucket_quartiles"),
    )
)
