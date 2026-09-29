from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level") != "L3"), "left_anti")
)

result = kept.select("id", "dept_name", "budget")
