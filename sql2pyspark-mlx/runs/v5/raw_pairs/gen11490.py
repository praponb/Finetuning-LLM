from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["location", "job_title"], ["location"], []],
        F.col("d.location").alias("location"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.salary")).alias("total_salary"),
        F.grouping_id().alias("gid"),
    )
)
