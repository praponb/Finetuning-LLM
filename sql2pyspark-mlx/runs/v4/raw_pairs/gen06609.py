from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.product_name"), F.col("p.launch_date"), F.col("p.category"))
)
