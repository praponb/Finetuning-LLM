from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 14800, "LARGE").otherwise("SMALL"))
    .cube("city", "credit_limit_tier")
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.round(F.avg("age"), 2).alias("avg_age_2"),
        F.countDistinct("country").alias("distinct_countries"),
        F.grouping("city").alias("grouping_level"),
    )
    .filter(F.col("consumer_count") < 1)
    .orderBy(F.col("city").desc())
)
