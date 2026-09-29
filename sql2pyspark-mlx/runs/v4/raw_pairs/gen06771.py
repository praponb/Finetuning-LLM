from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id")

scored = (
    df_orders
    .withColumn("running_amount", F.sum("amount").over(w_running))
    .withColumn("shipping_fee_bucket", F.ntile(10).over(w_recent))
)

result = (
    scored
    .groupBy(
        F.when(F.col("discount") >= 0.34, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.percentile_approx("running_amount", 0.95).alias("running_amount_p95"),
        F.avg("running_amount").alias("avg_running_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy(F.col("discount_tier").desc())
)
