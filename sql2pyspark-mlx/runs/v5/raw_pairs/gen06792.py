from pyspark.sql import functions as F

result = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.upper(F.col("x.email")).like("%IAM%")), "left_anti")
)
