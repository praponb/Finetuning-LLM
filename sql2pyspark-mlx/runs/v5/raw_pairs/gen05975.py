from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("salary") >= 144000)
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.budget"), F.col("t.min_salary"), F.col("t.salary_range"))
)
