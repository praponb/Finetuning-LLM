from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round((F.col("unit_price") * 1.1), 2).alias("unit_price_scaled"))
)

result = (
    base
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([10, 19, 33]))
        .select("id", "order_id")
    )
)
