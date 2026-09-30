from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "status").orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "status").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)

enriched = (
    df_orders
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
        F.sum("discount").over(w4).alias("cumulative_discount"),
        F.avg("shipping_fee").over(w5).alias("shipping_fee_avg_5"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cumulative_discount") >= 2, "PREMIUM").otherwise("STANDARD").alias("cumulative_discount_tier"),
        "customer_id",
        "status",
    )
    .agg(
        F.round(F.sum("amount_bucket") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("amount_bucket_per_mobile"),
        F.round(F.avg("running_amount"), 2).alias("avg_running_amount"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)
