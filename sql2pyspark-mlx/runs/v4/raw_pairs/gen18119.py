from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(~((F.col("ss.revenue") < 3500) | F.col("i.product_id").isin([2, 8])))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("ss.units"),
    )
    .orderBy("quantity")
)
