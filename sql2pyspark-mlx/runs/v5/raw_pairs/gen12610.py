from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", F.month("shipped_date").alias("shipped_month"), "status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy(F.col("shipped_month").desc())
)
