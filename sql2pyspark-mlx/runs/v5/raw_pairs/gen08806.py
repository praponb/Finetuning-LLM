from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 33)
    .select(
        "started_at",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)
