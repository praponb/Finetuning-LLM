from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
        F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 90, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

result = (
    valid
    .select("order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([2, 29, 31, 36]))
        .select("order_id")
    )
)
