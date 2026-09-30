from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("budget") >= 2339000) & F.col("location").isin(["Tokyo", "Bangkok", "Chiang Mai"]))
)

departments_stats = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3385000, "high").when(F.col("budget") >= 1197000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.count("*").alias("total_count"),
    )
)

result = (
    departments_stats
    .filter(F.col("total_count") < 6)
    .orderBy(F.col("min_budget").desc(), "budget_bucket", "dept_name")
    .limit(50)
)
