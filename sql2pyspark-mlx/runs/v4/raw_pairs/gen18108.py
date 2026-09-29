from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.customer_id") == 3)
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.when(F.col("p.stock_qty") >= 335, "high").when(F.col("p.stock_qty") >= 134, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .orderBy("stock_qty_bucket", "launch_cohort")
    .limit(5)
)
