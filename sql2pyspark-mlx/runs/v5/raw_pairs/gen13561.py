from pyspark.sql import functions as F

result = (
    df_stores
    .select(F.col("opened_date").alias("event_date"))
    .unionByName(df_shipments.select(F.col("delivered_date").alias("event_date")))
    .distinct()
    .orderBy(F.col("event_date").desc())
)
