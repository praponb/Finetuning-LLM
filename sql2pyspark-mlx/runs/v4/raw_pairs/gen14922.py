from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 150) & F.col("order_id").isin([20, 21, 34]))
    .select(
        "id",
        "quantity",
        "product_id",
        F.col("quantity").cast("double").alias("quantity_str"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)
