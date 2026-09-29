from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Analyst", "Engineer"]) & F.col("manager_id").isin([18, 19, 22, 27]))
    .groupBy("job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
    )
    .filter(F.col("intern_count") < 31)
)
