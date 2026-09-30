from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .filter(F.col("ssbs.total_count") > 10)
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("ssbs.total_units"),
        F.col("ssbs.total_count"),
    )
    .orderBy(F.col("total_units").desc())
)
