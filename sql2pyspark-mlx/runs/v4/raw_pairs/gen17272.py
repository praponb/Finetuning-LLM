from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(F.count("*").alias("shipment_count"))
    .orderBy("status")
)
