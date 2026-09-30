from pyspark.sql import functions as F

base = df_employees.filter(F.col("full_name").like("O%") & F.col("manager_id").isNull())

employees_stats = (
    base
    .groupBy("job_title")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("avg_salary") > 54)
    .select("job_title", "min_hire_date", "avg_salary")
    .orderBy(F.col("min_hire_date").desc())
)
