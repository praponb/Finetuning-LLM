from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 6)
    .select(
        "id",
        "order_id",
        "product_id",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)
