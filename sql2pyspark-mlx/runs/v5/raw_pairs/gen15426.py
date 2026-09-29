from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "order_id",
        "product_id",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
        F.floor("quantity").alias("quantity_whole"),
    )
)
