from pyspark.sql import functions as F

src = df_order_items.filter(~((F.col("quantity") <= 6) & (F.col("order_id") == 27)))

agg = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 390, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    agg
    .select("unit_price_bucket", "avg_unit_price")
    .orderBy(F.col("unit_price_bucket").desc())
)
