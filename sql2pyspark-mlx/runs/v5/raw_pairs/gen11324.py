from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("cost_range") >= 28)
    .select("status", "min_delivered_date", "cost_range")
)
