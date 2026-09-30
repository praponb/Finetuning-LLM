from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .filter(F.col("partner_count") > 28)
    .drop("partner_count")
    .orderBy("amount_count")
)
