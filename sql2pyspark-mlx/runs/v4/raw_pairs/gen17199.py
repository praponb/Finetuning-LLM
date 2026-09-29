from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
    )
)
