from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(~(F.col("i.store_id").isin([1, 5, 6]) & (F.col("i.product_id") == 10)))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.region"),
    )
)
