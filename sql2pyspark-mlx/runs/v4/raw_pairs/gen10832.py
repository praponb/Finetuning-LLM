from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("cost").alias("cost_count"),
        F.max("shipped_date").alias("latest_shipped_date"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
    .filter(F.col("kerry_count") >= 54)
    .drop("kerry_count")
    .orderBy(F.col("cost_count").desc())
)
