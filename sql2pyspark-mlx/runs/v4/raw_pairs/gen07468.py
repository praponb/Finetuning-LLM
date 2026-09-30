from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("quantity_p50") >= 0)
    .orderBy("quantity_stddev")
)
