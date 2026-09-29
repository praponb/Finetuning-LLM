from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(~(F.col("budget").between(3424000, 3890000) | (F.col("budget") <= 2654000)))
    .withColumn("location_length", F.length("location"))
)

departments_stats = (
    valid_departments
    .groupBy(
        F.when(F.col("location_length") >= 76, "high").when(F.col("location_length") >= 19, "mid").otherwise("low").alias("location_length_bucket"),
    )
    .agg(F.max("budget").alias("max_budget"))
)

result = (
    departments_stats
    .filter(F.col("max_budget") >= 2074000)
    .orderBy("location_length_bucket")
)
