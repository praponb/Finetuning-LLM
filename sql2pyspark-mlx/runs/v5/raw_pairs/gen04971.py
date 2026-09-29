from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("ss.product_id"),
        F.when(F.col("ss.revenue") >= 3900, "High").when(F.col("ss.revenue") >= 1400, "Medium").otherwise("Low").alias("revenue_tier"),
    )
    .orderBy(F.col("product_id").desc())
)
