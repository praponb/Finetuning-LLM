from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.cost") >= 340)
    .select(
        "src.id",
        "src.product_id",
        "src.quantity",
        "src.unit_price",
        F.col("b.product_name"),
        F.col("b.launch_date"),
    )
)
