from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.salary_range") >= 25)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("t.salary_range"),
    )
    .orderBy("budget", F.col("salary_range").desc())
)
