from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("quantity_count") >= 3)
    .orderBy("total_quantity")
)
