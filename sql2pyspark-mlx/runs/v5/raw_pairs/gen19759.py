from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") & ((F.col("plan") == "enterprise") | (F.col("status") == "PAUSED")))
    .withColumn("status_length", F.length("status"))
)

summary = (
    filtered_subscriptions
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
    )
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.sum("status_length").alias("total_status_length"),
    )
)

result = (
    summary
    .select("end_cohort", "min_end_date", "total_status_length")
    .orderBy("total_status_length")
)
