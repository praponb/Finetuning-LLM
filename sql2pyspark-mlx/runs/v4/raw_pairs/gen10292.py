from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.units") >= 28) | (F.col("ss.revenue") >= 1700))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("ss.revenue"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_date_label"),
    )
    .orderBy(F.col("id").desc())
)
