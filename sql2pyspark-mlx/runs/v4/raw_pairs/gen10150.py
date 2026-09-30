from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.col("store_sales.product_id").isin([1, 4]))
    .groupBy(
        F.when(F.col("stores.region").isin(["North", "Central"]), "priority").otherwise("other").alias("region_group"),
    )
    .agg(F.max(F.col("store_sales.units")).alias("max_units"))
)
