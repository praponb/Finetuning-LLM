from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("unit_price_filled_tier").orderBy(F.col("unit_price_filled_p90").desc())

base = (
    df_order_items
    .filter(F.col("unit_price") >= 260)
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

result = (
    base
    .groupBy(
        F.when(F.col("unit_price_filled") >= 61, "PREMIUM").otherwise("STANDARD").alias("unit_price_filled_tier"),
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("unit_price_filled", 0.9).alias("unit_price_filled_p90"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("unit_price_filled").alias("min_unit_price_filled"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("min_unit_price_filled") >= 0)
    .withColumn("unit_price_filled_p90_rank", F.rank().over(w))
)
