from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .filter(F.col("store_sales.sale_date").between("2024-06-24", "2025-02-24"))
    .select(
        F.col("store_sales.store_id"),
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("stores.city"),
    )
    .orderBy(F.col("store_id").desc())
)
