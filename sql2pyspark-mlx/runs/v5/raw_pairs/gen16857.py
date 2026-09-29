from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .filter(F.col("D.salary_range") > 68)
    .select(F.col("d.budget"), F.col("D.max_salary"), F.col("D.salary_range"))
    .orderBy(F.col("max_salary").desc(), "budget")
)
