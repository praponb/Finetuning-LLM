from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 93)
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.avg("cost").alias("avg_cost"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
