from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([21, 40]) & F.col("unit_price").between(190, 380))
    .select(
        "id",
        "quantity",
        "product_id",
        "order_id",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total_2"),
    )
    .orderBy(F.col("order_id").desc())
)
