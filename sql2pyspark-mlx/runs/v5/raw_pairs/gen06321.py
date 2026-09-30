from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller") & F.col("supplier_id").isNotNull())
    .cube("category", "brand")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("brand").desc())
)
