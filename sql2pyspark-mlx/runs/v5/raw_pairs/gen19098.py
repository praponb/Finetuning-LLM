from pyspark.sql import functions as F

kept = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id") == 38), "left_anti")
)

result = (
    kept
    .groupBy("city")
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .filter(F.col("consumer_count") >= 1)
    .orderBy(F.col("new_york_events").asc_nulls_first())
)
