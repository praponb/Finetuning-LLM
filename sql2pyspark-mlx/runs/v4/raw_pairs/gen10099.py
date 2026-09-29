from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("days_since_start_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("days_since_start_date").asc_nulls_last())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isin([1, 27, 35, 37]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lead("days_since_start_date", 1).over(w).alias("next_days_since_start_date"),
        F.rank().over(w2).alias("days_since_start_date_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("end_date") > F.to_date(F.lit("2024-05-12")))
    .withColumn("end_date_label", F.date_format("end_date", "MMM yyyy"))
)

stats = (
    valid
    .groupBy(
        F.when(F.col("days_since_start_date") >= 65, "PREMIUM").otherwise("STANDARD").alias("days_since_start_date_tier"),
    )
    .agg(
        F.avg("days_since_start_date_rank").alias("avg_days_since_start_date_rank"),
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("status") == "PAUSED", F.col("next_days_since_start_date"))).alias("paused_avg_next_days_since_start_date"),
    )
)

result = (
    stats
    .select("days_since_start_date_tier", "paused_avg_next_days_since_start_date")
    .orderBy("paused_avg_next_days_since_start_date")
)
