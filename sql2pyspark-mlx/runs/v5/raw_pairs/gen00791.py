from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("*").alias("row_count"),
    )
    .orderBy("max_unit_price")
)
