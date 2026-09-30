from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.month(F.col("ss.sale_date")).isin([3, 7, 10]) & F.col("ss.product_id").isin([1, 2, 5, 7]))
    .select(F.col("i.quantity"), F.col("ss.product_id"))
    .orderBy(F.col("product_id").desc(), F.col("quantity").desc())
)
