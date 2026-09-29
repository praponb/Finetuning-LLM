from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("shipping_fee"))).alias("mobile_avg_shipping_fee"),
    )
    .filter(F.col("amount_stddev") > 0)
)
