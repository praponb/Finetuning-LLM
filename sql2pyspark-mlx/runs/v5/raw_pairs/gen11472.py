from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 140)
    .select(
        "unit_price",
        "order_id",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
    .orderBy(F.col("order_id").desc())
)
