from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("p.brand"),
    )
    .orderBy("brand", F.col("price").desc())
)
