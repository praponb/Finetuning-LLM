from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.launch_date").between("2023-04-01", "2024-05-01"))
    .groupBy(F.col("p.brand"))
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .filter(F.col("total_quantity") > 1360)
)
