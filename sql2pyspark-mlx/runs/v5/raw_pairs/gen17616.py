from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .groupBy("status")
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy("status", F.col("cost_count").desc())
)
