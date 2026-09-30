from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "customer_id").orderBy("discount", "id")

metrics = (
    df_orders
    .filter(F.col("amount") > 1400)
    .select(
        "*",
        F.avg("shipping_fee").over(w).alias("running_avg_shipping_fee"),
        F.sum("shipping_fee").over(w).alias("running_sum_shipping_fee"),
        F.max("shipping_fee").over(w).alias("running_max_shipping_fee"),
        F.count("shipping_fee").over(w).alias("running_count_shipping_fee"),
        F.lag("discount", 1).over(w2).alias("prev_discount"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
        F.date_trunc("month", "order_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("running_count_shipping_fee", 0.95).alias("running_count_shipping_fee_p95"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
