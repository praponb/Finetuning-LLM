from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "product_id",
        "unit_price",
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)
