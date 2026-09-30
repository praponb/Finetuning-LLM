from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
    .orderBy("carrier", "latest_shipped_date")
    .limit(10)
)
