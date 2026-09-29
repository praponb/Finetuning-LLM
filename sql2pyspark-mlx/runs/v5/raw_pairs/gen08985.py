from pyspark.sql import functions as F

recent_employees = df_employees.filter(~F.col("is_remote"))

agg = (
    recent_employees
    .groupBy(
        F.when(F.col("salary") >= 124000, "high").when(F.col("salary") >= 67000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)

result = (
    agg
    .select("salary_bucket", "pct_manager")
    .orderBy("pct_manager")
)
