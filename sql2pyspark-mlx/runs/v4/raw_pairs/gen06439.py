from pyspark.sql import functions as F

joined = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select("src.*", F.col("e.job_title").alias("employee_job_title"))
)

result = (
    joined
    .withColumn("salary_tier", F.when(F.col("salary") >= 150000, "HIGH").otherwise("LOW"))
    .cube("salary_tier")
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.avg(F.when(F.col("level") == "L4", F.col("salary"))).alias("l4_avg_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_rows"),
        F.count("*").alias("cnt"),
        F.grouping("salary_tier").alias("gid"),
    )
)
