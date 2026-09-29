from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
    .filter(F.col("pct_l5") > 47)
)
