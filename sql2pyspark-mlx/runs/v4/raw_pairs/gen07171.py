from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2024-02-22").cast("date")) & (F.col("cost") < 660))
    .select(
        "*",
        F.when(F.col("category") == "Books", 1).otherwise(0).alias("is_books"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

result = (
    filtered
    .groupBy(
        "supplier_id",
        F.when(F.col("stock_qty") >= 380, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
    )
    .orderBy("supplier_id")
)
