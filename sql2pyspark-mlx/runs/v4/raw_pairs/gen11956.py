from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") < 340)
)
