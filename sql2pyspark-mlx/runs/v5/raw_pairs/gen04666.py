from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.lower("full_name").like("%ia %"))
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("DepartmentStats"), F.col("departments.id") == F.col("DepartmentStats.dept_id"), "inner")
    .filter(F.col("DepartmentStats.max_salary") >= 141000)
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("DepartmentStats.max_salary"),
        F.col("DepartmentStats.latest_hire_date"),
    )
)
