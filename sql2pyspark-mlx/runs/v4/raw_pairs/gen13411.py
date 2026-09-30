from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-08-15")
    .groupBy("job_title")
    .agg(F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"))
    .filter(F.col("manager_count") < 4)
    .select("job_title", "manager_count")
    .orderBy("job_title", F.col("manager_count").desc())
)
