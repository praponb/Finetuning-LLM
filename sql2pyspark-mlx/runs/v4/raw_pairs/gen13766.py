from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
    )
    .filter(F.col("avg_cost_2") >= 0)
)
