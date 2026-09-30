from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "tablet") & F.col("country").isin(["UK", "US", "SG"]))
    .select("id", "customer_id", F.upper("device").alias("device_upper"))
    .orderBy("customer_id", F.col("id").desc())
)
