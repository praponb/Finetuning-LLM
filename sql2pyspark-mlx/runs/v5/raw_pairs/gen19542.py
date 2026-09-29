from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("intern_count") > 33)
    .select("manager_id", "job_title", "intern_count", "min_salary")
)
