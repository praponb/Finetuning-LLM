from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.add_months(F.col("ss.sale_date"), 12).alias("sale_date_plus_months"),
    )
    .orderBy(F.col("quantity").desc())
)
