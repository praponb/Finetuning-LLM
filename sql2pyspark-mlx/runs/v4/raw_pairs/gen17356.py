from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-19").cast("date")) & (F.col("job_title") == "Analyst"))
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

result = (
    cleaned
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("is_remote_flag")).otherwise(0)).alias("l5_is_remote_flag"),
        F.round(F.avg("is_remote_flag"), 2).alias("avg_is_remote_flag"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("l5_is_remote_flag") < 0)
    .orderBy(F.col("level").asc_nulls_first())
)
