from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy("min_shipped_date")
)
