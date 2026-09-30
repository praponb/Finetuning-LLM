from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-06-09").cast("date"))
    .withColumn("is_analyst", F.when(F.col("job_title") == "Analyst", 1).otherwise(0))
    .withColumn("level_clean", F.coalesce("level", F.lit("OTHER")))
)

result = (
    filtered
    .rollup("level")
    .agg(
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("is_analyst")).otherwise(0)).alias("analyst_is_analyst"),
        F.percentile_approx("is_analyst", [0.25, 0.5, 0.75]).alias("is_analyst_quartiles"),
        F.grouping("level").alias("grouping_level"),
    )
    .orderBy("analyst_is_analyst")
)
