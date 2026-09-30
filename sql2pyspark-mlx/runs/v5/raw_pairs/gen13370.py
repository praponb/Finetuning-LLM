from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "ivy chen")
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.level").alias("employee_level"),
        F.col("p.job_title").alias("employee_job_title"),
        F.col("a.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.days_since_hire_date")).alias("avg_days_since_hire_date"),
    )
    .orderBy("employee_job_title")
)
