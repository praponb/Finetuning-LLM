from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") >= 400)
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
    .orderBy("launch_cohort")
)
