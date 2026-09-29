from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2023-05-10") & (F.col("job_title") == "Designer"))
    .select("id", "level", "salary")
)
