from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-02-23 03:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.hour("started_at").alias("started_at_hour"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.segment"), F.col("d.country"), F.col("d.name"))
)
