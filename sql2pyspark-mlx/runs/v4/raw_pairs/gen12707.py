from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "is_remote")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.min("hire_date").alias("min_hire_date"),
    )
)
