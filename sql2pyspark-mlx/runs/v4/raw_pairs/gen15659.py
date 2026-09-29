from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 150000, "high").when(F.col("salary") >= 73000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.count("*").alias("num_employees"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
    )
)
