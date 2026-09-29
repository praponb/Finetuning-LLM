from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        "order_id",
        "quantity",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.col("quantity").cast("string").alias("quantity_num_2"),
    )
    .orderBy("quantity_num_2", "id")
    .limit(50)
)
