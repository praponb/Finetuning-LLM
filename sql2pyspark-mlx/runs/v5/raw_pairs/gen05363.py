from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("full_name").like("%Mia%") | (F.col("manager_id") == 5)))
    .groupBy("is_remote")
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary")).otherwise(0)).alias("intern_salary"),
    )
    .orderBy(F.col("intern_salary").desc())
)
