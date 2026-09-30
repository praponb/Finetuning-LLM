from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.price").isNull())
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("p.cost"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.weekofyear(F.col("o.order_date")).alias("order_date_weekofyear"),
    )
    .orderBy("order_id", "product_id")
)
