from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("quantity") >= 243, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.date_trunc("WEEK", "last_restock").alias("week_start"),
    )
    .agg(
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .orderBy(F.col("week_start").asc_nulls_last())
)
