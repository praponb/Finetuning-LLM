from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([22, 30, 32]))
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)
