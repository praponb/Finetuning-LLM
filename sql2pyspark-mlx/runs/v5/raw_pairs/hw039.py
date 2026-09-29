from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(F.col("e.full_name").alias("emp_name"), F.col("m.full_name").alias("manager_name"))
)
