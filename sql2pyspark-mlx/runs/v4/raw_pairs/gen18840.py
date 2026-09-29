from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .filter(F.col("department_stats.max_salary") >= 107000)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("department_stats.salary_range"),
        F.col("department_stats.max_salary"),
    )
    .orderBy("location", F.col("max_salary").desc())
)
