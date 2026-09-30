from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category").isin(["Sports", "Home"]))
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.countDistinct(F.col("p.stock_qty")).alias("unique_suppliers"),
        (F.max(F.col("p.cost")) - F.min(F.col("p.cost"))).alias("cost_range"),
    )
    .orderBy(F.col("unique_suppliers").desc())
)
