from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.category"),
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .orderBy("unique_orders", "category", "launch_cohort")
    .limit(5)
)
