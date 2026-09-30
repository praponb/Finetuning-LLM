from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quantity_tier").orderBy(F.col("quantity_bit_or").desc())

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.when(F.col("unit_price") >= 310, "HIGH").otherwise("LOW").alias("unit_price_tier"),
        "order_id",
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .withColumn("quantity_bit_or_percent_rank", F.percent_rank().over(w))
    .orderBy("unit_price_tier")
)
