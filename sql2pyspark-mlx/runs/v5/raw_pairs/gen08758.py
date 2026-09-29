from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .withColumn("job_title_full_name", F.concat("job_title", F.lit(", "), "full_name"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("N/A")))
)

result = (
    cleaned
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.location").alias("location"),
        F.col("a.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.salary")).alias("total_salary"),
    )
)
