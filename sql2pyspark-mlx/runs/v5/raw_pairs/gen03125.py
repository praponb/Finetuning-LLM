from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(
        (F.max(F.col("store_sales.revenue")) - F.min(F.col("store_sales.revenue"))).alias("revenue_range"),
        F.max(F.col("store_sales.sale_date")).alias("last_sale_date"),
    )
    .orderBy(F.col("last_sale_date").desc())
)
