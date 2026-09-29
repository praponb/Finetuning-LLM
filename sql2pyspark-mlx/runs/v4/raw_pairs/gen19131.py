from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_scaled"),
    )
    .orderBy("unit_price", "id")
    .limit(20)
)
