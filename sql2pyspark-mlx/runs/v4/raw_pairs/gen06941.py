from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("country").isNull())
    .select(
        "*",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.concat_ws("-", "device", "browser").alias("device_browser_key"),
    )
)

result = (
    valid
    .groupBy("customer_id", "device")
    .pivot("country", ["TH", "JP"])
    .agg(F.max("days_since_started_at"))
    .select("customer_id", "device", F.col("TH").alias("th"), F.col("JP").alias("jp"))
)
