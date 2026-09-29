from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.salary_range") >= 69)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("t.latest_hire_date"),
        F.col("t.salary_range"),
    )
    .orderBy(F.col("id").desc(), "latest_hire_date")
    .limit(10)
)
