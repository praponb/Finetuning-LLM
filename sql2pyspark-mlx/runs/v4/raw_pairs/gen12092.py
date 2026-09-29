from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_month"),
        F.col("ss.product_id"),
    )
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
)
