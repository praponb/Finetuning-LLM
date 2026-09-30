from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") == "Berlin")
    .select(F.col("d.dept_name"), F.col("e.job_title"), F.col("e.manager_id"))
    .orderBy(F.col("manager_id").desc(), "dept_name")
)
