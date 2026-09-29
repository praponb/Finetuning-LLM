from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
    )
    .orderBy(F.col("quantity_adjusted").desc(), F.col("id").desc())
    .limit(5)
)
