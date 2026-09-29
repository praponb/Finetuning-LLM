from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"))
    .filter(F.col("intern_count") < 7)
)
