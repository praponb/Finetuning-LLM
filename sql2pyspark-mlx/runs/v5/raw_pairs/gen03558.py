from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.when(F.col("cost") >= 83, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
    )
    .orderBy(F.col("avg_cost").desc())
)
