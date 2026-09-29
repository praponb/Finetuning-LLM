from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
    .filter(F.col("pct_designer") < 47)
)
