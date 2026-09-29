from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") > 94000), "left_anti")
)

result = (
    eligible
    .select("id", "dept_name", "location")
    .orderBy(F.col("id").desc())
)
