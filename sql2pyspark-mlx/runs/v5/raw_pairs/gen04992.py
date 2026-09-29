from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "customer_id",
        "device",
        F.quarter("started_at").alias("started_at_quarter"),
    )
)
