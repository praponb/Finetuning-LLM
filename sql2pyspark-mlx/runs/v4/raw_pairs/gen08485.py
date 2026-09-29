from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L3"]))
    .groupBy("job_title")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.min("hire_date").alias("min_hire_date"),
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
    )
    .filter(F.col("l3_count") < 70)
    .orderBy("last_hire_date", "job_title")
    .limit(3)
)
