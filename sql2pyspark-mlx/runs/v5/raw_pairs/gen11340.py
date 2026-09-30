from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .filter(F.col("shipping_fee") < 26)
    .select(
        "*",
        F.count("discount").over(w).alias("last3_count_discount"),
        F.avg("discount").over(w).alias("last3_avg_discount"),
        F.min("discount").over(w).alias("last3_min_discount"),
        F.sum("shipping_fee").over(w2).alias("cumulative_shipping_fee"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("shipping_fee") >= 43, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("cumulative_shipping_fee", 0.9).alias("cumulative_shipping_fee_p90"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
)
