from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([8, 17, 19, 26]))
    .groupBy("manager_id", "job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
        F.max("salary").alias("max_salary"),
    )
)
