from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("num_order_items"),
    )
)
