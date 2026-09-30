from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 8))
    .select(
        "quantity",
        F.round((F.col("unit_price") * 0.9), 2).alias("adjusted_unit_price"),
    )
    .orderBy(F.col("quantity").desc(), F.col("adjusted_unit_price").desc())
)
