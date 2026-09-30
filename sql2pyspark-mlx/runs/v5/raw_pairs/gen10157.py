from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        "status",
        F.when(F.col("discount") >= 0.45, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("channel") == "partner", F.col("amount"))).alias("partner_avg_amount"),
    )
)
