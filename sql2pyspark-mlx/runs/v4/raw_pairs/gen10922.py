from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.count("*").alias("inventory_count"),
    )
)
