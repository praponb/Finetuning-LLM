from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 143000, "high").when(F.col("salary") >= 70000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
    .filter(F.col("pct_l3") > 59)
    .select("salary_bucket", "avg_salary", "pct_l3")
    .orderBy(F.col("pct_l3").desc(), "salary_bucket")
    .limit(5)
)
