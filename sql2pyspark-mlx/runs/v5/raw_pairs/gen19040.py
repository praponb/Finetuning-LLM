from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("hire_date").between("2024-01-23", "2025-04-01") & F.col("email").like("%@outlook.com") & (F.col("manager_id") == 39))
)

agg = (
    recent_employees
    .groupBy(
        F.when(F.col("salary") >= 135000, "high").when(F.col("salary") >= 85000, "mid").otherwise("low").alias("salary_bucket"),
        "is_remote",
    )
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    agg
    .filter(F.col("l3_count") > 9)
    .select("salary_bucket", "is_remote", "l3_count", "salary_range")
)
