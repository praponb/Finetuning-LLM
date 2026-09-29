from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget").between(2815000, 3447000))
    .withColumn("dept_name_length", F.length("dept_name"))
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([5, 31, 32, 36])), "left_anti")
)
