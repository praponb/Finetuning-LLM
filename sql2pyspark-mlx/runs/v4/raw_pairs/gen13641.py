from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED"]))
    .select(F.col("delivered_date").alias("event_date"))
    .unionByName(df_products.select(F.col("launch_date").alias("event_date")))
    .distinct()
)
