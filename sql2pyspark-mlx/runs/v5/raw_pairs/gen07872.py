from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("orders.order_date")) < 2024, "before_2024").when(F.year(F.col("orders.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.col("orders.channel"),
    )
    .agg(
        F.max(F.col("order_items.quantity")).alias("max_quantity"),
        F.sum(F.col("order_items.quantity")).alias("total_quantity"),
    )
    .orderBy("channel", "max_quantity")
)
