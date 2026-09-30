from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(3, 5))
    .select("*", F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"))
)

result = (
    valid
    .select("id", "product_id", "unit_price", "unit_price_filled")
    .unpivot(
        ["id", "product_id"],
        ["unit_price", "unit_price_filled"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
