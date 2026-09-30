from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_inventory
    .withColumn("last3_count_quantity", F.count("quantity").over(w))
    .withColumn("last3_min_quantity", F.min("quantity").over(w))
    .withColumn("last3_sum_quantity", F.sum("quantity").over(w))
)

result = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"))
    .agg(
        F.percentile_approx("last3_count_quantity", [0.25, 0.5, 0.75]).alias("last3_count_quantity_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("last3_min_quantity", 0.5).alias("last3_min_quantity_p50"),
        F.round(F.avg("last3_min_quantity"), 2).alias("avg_last3_min_quantity"),
        F.percentile_approx("last3_min_quantity", [0.25, 0.5, 0.75]).alias("last3_min_quantity_quartiles"),
    )
)
