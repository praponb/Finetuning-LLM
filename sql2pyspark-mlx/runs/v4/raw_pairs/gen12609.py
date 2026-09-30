from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
    .orderBy(F.col("greatest_quantity_unit_price").desc(), "id")
    .limit(50)
)
