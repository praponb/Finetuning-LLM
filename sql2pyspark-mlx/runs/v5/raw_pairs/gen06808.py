from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("cost") < 530).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("i.last_restock") <= "2024-02-24")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
    )
    .orderBy("id")
    .limit(50)
)
