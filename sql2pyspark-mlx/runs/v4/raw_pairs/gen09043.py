from pyspark.sql import functions as F

with_dims = (
    df_orders
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.store_name"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
