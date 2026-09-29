from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") != "desktop")
    .select(
        "id",
        "country",
        "browser",
        "customer_id",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
