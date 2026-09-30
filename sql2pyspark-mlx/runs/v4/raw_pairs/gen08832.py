from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Analyst") & (F.col("level") == "L2"))
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
)
