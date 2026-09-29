from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(
        F.when(F.col("cost") >= 74, "LARGE").otherwise("SMALL").alias("cost_tier"),
        F.date_trunc("MONTH", "delivered_date").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.avg(F.when(F.col("carrier") == "DHL", F.col("cost"))).alias("dhl_avg_cost"),
    )
)
