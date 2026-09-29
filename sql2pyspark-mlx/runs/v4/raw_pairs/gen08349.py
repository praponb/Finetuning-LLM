from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(
        F.when(F.col("cost") >= 94, "HIGH").otherwise("LOW").alias("cost_tier"),
        "carrier",
    )
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
    )
)
