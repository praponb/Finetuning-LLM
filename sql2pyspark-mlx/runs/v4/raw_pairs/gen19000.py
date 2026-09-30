from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-10-02").cast("date")) & F.col("status").isin(["SHIPPED", "COMPLETED"]))
    .select("*", F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("discount") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
    )
    .orderBy(F.col("pending_rows").desc())
)
