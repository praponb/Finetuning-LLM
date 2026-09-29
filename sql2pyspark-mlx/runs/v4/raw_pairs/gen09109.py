from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.round((F.col("unit_price") * 1.15), 2).alias("adjusted_unit_price"),
    )
    .orderBy(F.col("order_id").desc())
)
