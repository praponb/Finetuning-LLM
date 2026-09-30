from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("amount", "id")

metrics = (
    df_orders
    .select(
        "*",
        F.first("discount", ignorenulls=True).over(w).alias("first_known_discount"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.ntile(10).over(w3).alias("amount_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("shipping_fee") >= 41, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
        "channel",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("status") == "COMPLETED", F.col("amount_bucket"))).alias("completed_max_amount_bucket"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("channel").desc())
)
