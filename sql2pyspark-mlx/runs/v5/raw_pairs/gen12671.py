from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .groupBy(
        F.when(F.year(F.col("orders.order_date")) < 2024, "before_2024").when(F.year(F.col("orders.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        (F.max(F.col("order_items.unit_price")) - F.min(F.col("order_items.unit_price"))).alias("unit_price_range"),
    )
    .orderBy(F.col("unit_price_range").desc(), "order_cohort")
    .limit(1)
)
