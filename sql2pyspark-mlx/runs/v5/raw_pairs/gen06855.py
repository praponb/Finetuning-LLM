from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.last_restock").between("2024-01-01", "2024-10-19"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("product_id").desc(), F.col("id").desc())
)
