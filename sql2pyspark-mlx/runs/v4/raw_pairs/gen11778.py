from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2023-03-01") | (F.col("job_title") == "Intern"))
    .groupBy("is_remote", "job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.countDistinct("salary").alias("unique_levels"),
    )
)
