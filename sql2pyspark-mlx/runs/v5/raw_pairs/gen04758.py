from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "product_id",
        "order_id",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)
