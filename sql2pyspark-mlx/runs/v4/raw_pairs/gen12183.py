from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy("cost_count", "fedex_count")
)
