from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.cost") >= 790, "high").when(F.col("p.cost") >= 120, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
)
