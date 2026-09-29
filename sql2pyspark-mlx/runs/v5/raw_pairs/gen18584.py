from pyspark.sql import functions as F

result = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") <= 113000), "left_anti")
)
