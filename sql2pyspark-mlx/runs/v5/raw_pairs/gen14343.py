from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 6)
    .select("status", "cost_count", "cnt")
)
