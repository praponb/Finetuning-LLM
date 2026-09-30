from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"))
    .filter(F.col("analyst_count") < 2)
    .orderBy("manager_id")
)
