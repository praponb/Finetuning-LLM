from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("year_start", F.date_trunc("year", "last_restock"))
    .rollup("year_start", "product_id")
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy("quantity_p50")
)
