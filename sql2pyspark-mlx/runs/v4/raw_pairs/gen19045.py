from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id") == 18)
    .groupBy(F.col("s.city"), F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
)
