from pyspark.sql import functions as F

result = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").like("%bob%")), "left_anti")
)
