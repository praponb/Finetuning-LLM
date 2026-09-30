from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.when(F.col("cost") >= 88, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_per_delivered"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("cost_per_delivered").desc_nulls_last())
)
