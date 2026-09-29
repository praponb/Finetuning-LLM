from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("delivered_date") >= "2023-07-01") & (F.col("shipped_date") <= "2023-10-01"))
)

shipments_stats = (
    recent_shipments
    .groupBy("carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    shipments_stats
    .select("carrier", "avg_cost", "total_cost")
    .orderBy(F.col("avg_cost").desc(), "carrier")
)
