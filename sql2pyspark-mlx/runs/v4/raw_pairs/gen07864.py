from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("num_inventory") >= 5)
)
