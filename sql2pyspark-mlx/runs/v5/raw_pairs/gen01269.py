from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount", "id")
w3 = Window.orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("amount_tier").orderBy(F.col("num_status").desc())

with_windows = (
    df_orders
    .select(
        "*",
        F.last("discount", ignorenulls=True).over(w).alias("last_known_discount"),
        F.lead("shipping_fee", 2).over(w2).alias("next_shipping_fee"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("amount") >= 1320, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("amount_share"), 2).alias("avg_amount_share"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("amount_per_mobile"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("channel") == "web", F.col("amount_share"))).alias("web_max_amount_share"),
        F.stddev_samp("discount").alias("discount_stddev"),
    )
    .withColumn("num_status_dense_rank", F.dense_rank().over(w5))
)
