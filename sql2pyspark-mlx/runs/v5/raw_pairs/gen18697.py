from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("location") == "Singapore")

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3781000, "high").when(F.col("budget") >= 1895000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["Bangkok", "London"]), "group_a").otherwise("other").alias("location_group"),
    )
    .agg(F.count("budget").alias("budget_count"))
)

result = (
    budget_bucket_summary
    .filter(F.col("budget_count") < 6)
    .select("budget_bucket", "location_group", "budget_count")
)
