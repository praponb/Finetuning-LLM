from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-08-11").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.array_contains("tags", "clearance").alias("is_clearance"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

result = (
    cleaned
    .select("id", "category", "price", F.col("cost").alias("COST_VAL"))
    .unpivot(["id", "category"], ["price", "COST_VAL"], "measure", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)
