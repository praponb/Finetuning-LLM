from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2025-02-01") & (F.col("shipped_date") > "2024-04-01") & (F.col("carrier") == "FedEx"))
    .groupBy("carrier")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)
