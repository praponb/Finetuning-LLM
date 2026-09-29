from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.sale_date") <= "2024-08-01") | F.col("i.store_id").isin([1, 6]))
    .select(
        F.col("i.last_restock"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
    )
    .orderBy(F.col("product_id").desc(), F.col("units").desc())
)
