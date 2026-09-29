from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.dept_name"), F.col("d.location"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.count("*").alias("num_rows"),
        F.avg("salary").alias("avg_salary"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
    )
)
