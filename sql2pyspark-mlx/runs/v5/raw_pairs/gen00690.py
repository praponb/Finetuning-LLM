from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy(F.col("cost_range").desc(), "status")
    .limit(10)
)
