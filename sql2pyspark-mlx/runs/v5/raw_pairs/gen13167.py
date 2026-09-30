from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(~F.col("job_title").isin(["Analyst", "Manager", "Designer"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.dept_name").isin(["Sales", "Legal", "HR"]))
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.dept_name"))
)
