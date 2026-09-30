from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        "customer_id",
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("started_at").alias("last_started_at"),
        F.max("started_at").alias("ended_at"),
    )
    .orderBy("customer_id")
)
