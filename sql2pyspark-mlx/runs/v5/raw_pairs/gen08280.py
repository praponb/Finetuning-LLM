from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.cost") > 200)
    .groupBy(
        F.when(F.col("p.cost") >= 720, "high").when(F.col("p.cost") >= 130, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
)
