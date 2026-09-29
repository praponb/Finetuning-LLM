from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id").isin([13, 18, 22, 29]) & (F.col("order_id") == 17)) | F.col("order_id").isin([2, 5, 6, 9]))
    .select(
        "id",
        "product_id",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)
