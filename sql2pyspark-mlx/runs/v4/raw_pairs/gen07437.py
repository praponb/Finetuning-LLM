from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles_2"),
    )
)
