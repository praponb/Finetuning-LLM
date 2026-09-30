from pyspark.sql import functions as F

base = df_shipments.filter(F.col("cost") < 65)

agg = (
    base
    .groupBy("status")
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
)

result = agg.select("status", "latest_shipped_date")
