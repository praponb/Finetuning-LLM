from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(~F.col("status").isin(["LOST", "IN_TRANSIT"]))

summary = (
    recent_shipments
    .groupBy("status", F.year("delivered_date").alias("delivered_year"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    summary
    .filter(F.col("avg_cost") >= 28)
    .orderBy(F.col("status").desc(), F.col("cost_range").desc())
)
