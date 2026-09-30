from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.min_salary") > 119000)
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ds.salary_range"),
        F.col("ds.min_salary"),
    )
    .orderBy("location", F.col("min_salary").desc())
)
