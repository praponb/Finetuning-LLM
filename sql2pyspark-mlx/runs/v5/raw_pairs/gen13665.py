from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.status") != "SHIPPED")
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.col("p.category"),
    )
    .agg(
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .orderBy("category", "launch_cohort")
    .limit(5)
)
