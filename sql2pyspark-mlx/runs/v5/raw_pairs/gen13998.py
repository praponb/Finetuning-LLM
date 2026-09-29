from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("customer_id", F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
    )
    .orderBy(F.col("edge_events").asc_nulls_first())
)
