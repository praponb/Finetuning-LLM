from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .filter(F.col("quantity_range") > 40)
)
