from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("p.cost") >= 730, "high").when(F.col("p.cost") >= 120, "mid").otherwise("low").alias("cost_bucket"),
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.count("*").alias("total_count"),
    )
    .orderBy("launch_cohort")
)
