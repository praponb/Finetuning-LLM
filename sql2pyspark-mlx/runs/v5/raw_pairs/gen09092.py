from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.year("end_date").alias("end_year"))
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("num_subscriptions"),
    )
    .filter(F.col("num_subscriptions") > 2)
    .orderBy(F.col("total_count").desc(), F.col("num_subscriptions").desc())
)
