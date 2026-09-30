from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .select(
        "*",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)

result = (
    filtered
    .select("country", "id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id") == 13)
        .select("country", "id")
    )
)
