from pyspark.sql import functions as F

result = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.year(F.col("c.hire_date")) == 2024), "left_anti")
)
