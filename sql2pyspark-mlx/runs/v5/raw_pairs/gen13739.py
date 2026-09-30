from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("channel") == "web"))
    .select(
        "*",
        F.round((F.col("shipping_fee") * 1.07), 2).alias("shipping_fee_scaled"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("pending_rows") > 1)
)
