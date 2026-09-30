from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.product_id").isin([5, 9]) & F.col("i.quantity").between(146, 214))
    .select(F.col("i.last_restock"), F.col("ss.store_id"))
    .orderBy(F.col("store_id").desc(), F.col("last_restock").desc())
)
