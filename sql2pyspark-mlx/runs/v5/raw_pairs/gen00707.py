from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        F.col("p.category"),
        F.col("p.launch_date"),
    )
)
