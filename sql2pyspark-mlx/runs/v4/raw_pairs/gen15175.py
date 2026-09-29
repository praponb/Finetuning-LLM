from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.avg_salary"),
        F.col("t.first_hire_date"),
    )
    .orderBy("location")
)
