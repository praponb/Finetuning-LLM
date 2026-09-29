from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_last(), "id")

deduped = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
    )
)

result = (
    deduped
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("amount"))).alias("mobile_avg_amount"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.avg(F.when(F.col("status") == "PENDING", F.col("amount"))).alias("pending_avg_amount"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy(F.col("cancelled_rows").desc())
)
