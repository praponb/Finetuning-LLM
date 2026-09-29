from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-01-13").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.weekofyear("last_restock").alias("last_restock_weekofyear"),
    )
)

result = (
    cleaned
    .groupBy("store_id")
    .agg(
        F.percentile_approx("last_restock_weekofyear", 0.9).alias("last_restock_weekofyear_p90"),
        F.count("*").alias("event_count"),
        F.sum("last_restock_weekofyear").alias("total_last_restock_weekofyear"),
    )
)
