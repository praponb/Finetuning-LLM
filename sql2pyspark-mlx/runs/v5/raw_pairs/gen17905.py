from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .filter(F.col("D.salary_range") > 53)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("D.l5_salary"),
        F.col("D.salary_range"),
    )
    .orderBy(F.col("l5_salary").desc())
)
