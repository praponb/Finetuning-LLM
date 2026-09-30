from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("unit_price").between(200, 220) & F.col("unit_price").between(310, 350)))
    .select(
        "id",
        "product_id",
        "quantity",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)
