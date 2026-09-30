from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("i.product_id"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
    .orderBy("product_name", "launch_date")
)
