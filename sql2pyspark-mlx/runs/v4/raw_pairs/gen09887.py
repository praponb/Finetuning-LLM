from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue").between(1300, 1800) | F.col("ss.sale_date").between("2025-03-11", "2025-08-01"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.revenue"),
    )
    .orderBy("store_sale_store_id")
)
