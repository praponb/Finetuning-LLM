from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("store_sales.product_id").isin([1, 4, 9]))
    .groupBy(
        F.when(F.col("stores.sqft") >= 16451, "high").when(F.col("stores.sqft") >= 2636, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        (F.max(F.col("store_sales.units")) - F.min(F.col("store_sales.units"))).alias("units_range"),
        F.max(F.col("store_sales.sale_date")).alias("last_sale_date"),
    )
)
