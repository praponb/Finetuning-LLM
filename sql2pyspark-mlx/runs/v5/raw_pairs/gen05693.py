from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 8, 10]))
    .groupBy("store_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("ssbs.avg_units"),
    )
)
