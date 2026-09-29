from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.product_id").isin([2, 3, 4]))
    .groupBy(F.col("s.region"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") >= 34)
)
