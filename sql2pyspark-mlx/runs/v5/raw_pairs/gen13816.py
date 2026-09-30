from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("avg_unit_price").desc())

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .withColumn("avg_unit_price_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("product_id").desc_nulls_last())
)
