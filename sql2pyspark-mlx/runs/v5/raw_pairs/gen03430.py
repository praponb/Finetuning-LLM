from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy("latest_hire_date", F.col("dept_id").desc())
)
