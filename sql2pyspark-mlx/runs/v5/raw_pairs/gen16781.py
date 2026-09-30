from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") <= "2025-05-01") & F.col("carrier").isin(["FedEx", "Flash"]))
    .withColumn("carrier_lower", F.lower("carrier"))
)
