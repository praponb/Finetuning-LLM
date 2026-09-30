from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-09-01").cast("date")) & F.col("supplier_id").isNotNull() & F.array_contains("tags", "new"))
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.month("launch_date").alias("launch_date_month"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

result = (
    cleaned
    .groupBy(F.when(F.col("cost") >= 680, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("stock_qty")).otherwise(0)).alias("stark_stock_qty"),
        F.corr("cost", "stock_qty").alias("corr_cost_stock_qty"),
        F.sum(F.when(F.col("brand") == "Globex", F.col("stock_qty")).otherwise(0)).alias("globex_stock_qty"),
    )
)
