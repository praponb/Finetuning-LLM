from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("hire_date").between("2023-05-02", "2024-07-01"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "left")
    .filter(F.col("department_totals.director_salary") >= 77)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("department_totals.director_salary"),
    )
)
