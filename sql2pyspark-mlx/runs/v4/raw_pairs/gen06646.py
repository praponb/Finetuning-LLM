from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.year("hire_date") >= 2023)
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .filter(F.col("department_stats.salary_range") >= 38)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("department_stats.salary_range"),
    )
    .orderBy(F.col("budget").desc())
)
