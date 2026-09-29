from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "product_id",
        F.round((F.col("quantity") * 0.9), 2).alias("adjusted_quantity"),
    )
    .orderBy("adjusted_quantity")
)
