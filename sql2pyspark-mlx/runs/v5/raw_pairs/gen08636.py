from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter((F.col("p.price") >= 1100) & F.col("p.category").isin(["Books", "Grocery", "Sports"]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.cost"),
        F.col("s.city"),
        F.date_trunc("week", F.col("s.opened_date")).alias("opened_date_period"),
    )
    .orderBy("opened_date_period")
)
