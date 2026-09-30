from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("carrier").isin(["Kerry", "UPS", "Flash"]))

agg = (
    filtered_shipments
    .groupBy("status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
)

result = (
    agg
    .orderBy(F.col("latest_shipped_date").desc(), "status")
    .limit(3)
)
