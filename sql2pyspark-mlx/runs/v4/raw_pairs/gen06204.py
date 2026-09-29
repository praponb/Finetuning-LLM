from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-06-01", "2025-10-01"))
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("ssbs.min_units"),
    )
)
