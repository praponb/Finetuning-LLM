from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-09-12").cast("date"))
    .select(
        "*",
        F.round((F.col("amount") * 1.07), 2).alias("amount_adjusted"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("discount") >= 0.38, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("discount").alias("min_discount"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("status") == "PENDING", F.col("amount"))).alias("pending_avg_amount"),
    )
)
