from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.max_salary") > 130000)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ds.latest_hire_date"),
        F.col("ds.max_salary"),
    )
)
