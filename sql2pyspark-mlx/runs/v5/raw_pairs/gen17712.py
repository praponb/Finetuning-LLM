from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
    .filter(F.col("manager_count") < 1)
    .drop("manager_count")
)
