from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("budget") > 2909000)
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary").between(87000, 88000)), "left_anti")
)
