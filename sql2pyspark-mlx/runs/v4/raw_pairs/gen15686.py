from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.month("launch_date").isin([8, 9, 12])).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
