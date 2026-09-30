from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("hire_date") < "2024-09-12")

level_summary = (
    valid_employees
    .groupBy("level")
    .agg(F.max("hire_date").alias("last_hire_date"))
)

result = level_summary
