from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price").between(580, 1050))
    .select(
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("p.launch_date"),
    )
)
