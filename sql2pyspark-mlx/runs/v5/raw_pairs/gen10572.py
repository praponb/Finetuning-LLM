from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        F.when(F.col("cost") >= 108, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.avg(F.when(F.col("carrier") == "DHL", F.col("cost"))).alias("dhl_avg_cost"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("cost_per_in_transit"),
    )
)
