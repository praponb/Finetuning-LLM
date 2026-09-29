from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter((F.col("shipped_date") >= "2025-04-18") & F.col("shipped_date").between("2024-10-01", "2025-09-01") & F.col("carrier").isin(["FedEx", "Kerry"]))
)

summary = (
    filtered_shipments
    .groupBy("carrier")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)

result = (
    summary
    .select("carrier", "last_shipped_date")
    .orderBy(F.col("last_shipped_date").desc(), "carrier")
)
