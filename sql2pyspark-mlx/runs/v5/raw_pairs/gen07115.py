from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("hire_date") >= "2025-04-01")
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.director_count") >= 10)
    .select(F.col("d.budget"), F.col("d.dept_name"), F.col("t.director_count"))
    .orderBy("dept_name", "director_count")
)
