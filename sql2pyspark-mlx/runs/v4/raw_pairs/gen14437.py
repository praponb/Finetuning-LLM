from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.min_salary") >= 97000)
    .select(F.col("d.budget"), F.col("t.min_salary"), F.col("t.designer_count"))
)
