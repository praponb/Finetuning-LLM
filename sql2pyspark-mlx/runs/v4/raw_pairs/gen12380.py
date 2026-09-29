from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 310)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("quantity_range") < 70)
    .select("quantity_bucket", "unit_price_bucket", "quantity_range", "cnt")
    .orderBy(F.col("unit_price_bucket").desc(), "quantity_bucket")
)
