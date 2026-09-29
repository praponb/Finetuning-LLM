from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("i.product_id"),
        F.col("i.quantity"),
    )
)
