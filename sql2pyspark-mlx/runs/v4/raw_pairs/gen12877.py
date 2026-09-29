from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name").isin(["Sales", "Legal", "HR"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.hire_date") > "2023-09-03")
    .select(F.col("e.id"), F.col("e.level"), F.col("e.hire_date"), F.col("e.full_name"))
)
