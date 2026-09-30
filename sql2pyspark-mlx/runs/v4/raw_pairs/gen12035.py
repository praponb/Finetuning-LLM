from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("salary_tier").orderBy(F.col("l3_count").desc())

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws(",", "email", "level").alias("email_level_key"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("salary") >= 141000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
    .filter(F.col("l3_count") >= 1)
    .withColumn("l3_count_rank", F.rank().over(w))
)
