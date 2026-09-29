from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", F.month("hire_date").alias("hire_month"))
    .agg(F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"))
    .filter(F.col("l1_count") < 7)
    .orderBy(F.col("l1_count").desc(), "job_title", "hire_month")
    .limit(5)
)
