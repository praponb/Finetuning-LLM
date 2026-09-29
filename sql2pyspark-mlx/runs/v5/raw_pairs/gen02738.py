from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.lower("product_name").alias("product_name_lower"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

result = (
    filtered
    .rollup(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.date_trunc("YEAR", "launch_date_month_end").alias("year_start"),
        F.when(F.col("stock_qty") >= 377, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
        F.avg(F.when(F.col("category") == "Grocery", F.col("stock_qty"))).alias("grocery_avg_stock_qty"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50_2"),
    )
)
