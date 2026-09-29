from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("hire_date") <= "2025-09-02")
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("D.salary_range"),
        F.col("D.max_salary"),
    )
    .orderBy(F.col("max_salary").desc())
)
