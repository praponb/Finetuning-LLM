from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
    .filter(F.col("max_salary") < 119000)
    .select("level", "max_salary", "engineer_count")
)
