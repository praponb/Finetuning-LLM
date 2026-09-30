from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]) & (F.col("cost") >= 61))
    .groupBy(F.month("shipped_date").alias("shipped_month"), "status")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 78)
    .orderBy(F.col("status").desc())
)
