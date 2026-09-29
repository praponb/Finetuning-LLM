from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "right")
    .groupBy(F.col("departments.location"))
    .agg(F.count("*").alias("num_employees"))
    .orderBy("location")
)
