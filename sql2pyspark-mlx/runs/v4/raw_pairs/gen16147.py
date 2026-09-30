from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("unit_price") < 290)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

result = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.price"), F.col("p.product_name"))
)
