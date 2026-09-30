from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget") <= 3511000)
    .select("*", F.ceil("budget").alias("budget_whole"))
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.upper(F.col("ch.email")).like("%IAM%")), "left_anti")
)
