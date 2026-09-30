from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter((F.col("p.category") == "Electronics") & (F.col("p.cost") >= 280))
    .select(
        F.col("p.launch_date"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
    )
)
