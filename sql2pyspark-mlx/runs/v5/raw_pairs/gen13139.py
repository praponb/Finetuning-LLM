from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-06-10")
    .select(
        "customer_id",
        "device",
        F.quarter("started_at").alias("started_at_quarter"),
    )
    .orderBy("started_at_quarter", F.col("device").desc())
)
