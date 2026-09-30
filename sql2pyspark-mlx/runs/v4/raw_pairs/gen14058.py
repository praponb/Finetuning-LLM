from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")
w2 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("quantity_tier").orderBy(F.col("cnt").desc())

enriched = (
    df_order_items
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w2))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w3))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        F.when(F.col("last_known_quantity") >= 7, "LARGE").otherwise("SMALL").alias("last_known_quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("corr_unit_price_quantity", 0.95).alias("corr_unit_price_quantity_p95"),
    )
    .withColumn("cnt_rank", F.rank().over(w4))
    .orderBy(F.col("quantity_tier").desc_nulls_first())
)
