from pyspark.sql import functions as F

result = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("c.full_name"))) == "emma brown"), "left_anti")
)
