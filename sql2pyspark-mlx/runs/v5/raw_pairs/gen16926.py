from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(~(F.col("p.brand").isin(["Globex", "Umbrella", "Stark"]) & F.col("i.store_id").isin([3, 5, 6])))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
    )
    .orderBy("launch_date")
)
