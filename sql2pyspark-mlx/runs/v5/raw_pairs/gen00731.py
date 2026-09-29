from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("quantity") < 6)
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
)

result = (
    base
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id") == 14)
        .select("id", "product_id")
    )
)
