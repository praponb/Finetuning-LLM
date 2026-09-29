from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L3")
    .groupBy("job_title")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") >= 70)
    .select("job_title", "avg_salary")
    .orderBy("job_title", F.col("avg_salary").desc())
    .limit(10)
)
