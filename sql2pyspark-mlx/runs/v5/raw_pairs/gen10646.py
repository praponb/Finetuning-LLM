from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount")).otherwise(0)).alias("store_discount"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
    )
    .filter(F.col("shipping_fee_p90") < 1000000)
)
