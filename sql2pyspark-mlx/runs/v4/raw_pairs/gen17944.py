from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
    )
    .filter(F.col("engineer_count") >= 1)
    .orderBy(F.col("engineer_count").desc())
)
