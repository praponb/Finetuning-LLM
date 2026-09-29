from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("amount"))).alias("partner_total_amount"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee"))).alias("cancelled_avg_shipping_fee"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cancelled_avg_shipping_fee").asc_nulls_first())
)
