from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("country") == "SG")
    .withColumn("device_length", F.length("device"))
)

result = (
    valid
    .select("customer_id", "country")
    .exceptAll(
        df_sessions
        .filter(F.hour("started_at").between(10, 15))
        .select("customer_id", "country")
    )
)
