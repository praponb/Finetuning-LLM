from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
    )
)
