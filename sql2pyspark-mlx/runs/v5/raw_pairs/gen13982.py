from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 107, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
    .orderBy("ups_cost")
)
