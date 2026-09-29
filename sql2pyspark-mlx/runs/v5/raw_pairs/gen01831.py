from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "manager_id")
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("l1_count") > 21)
    .orderBy(F.col("manager_id").desc())
)
