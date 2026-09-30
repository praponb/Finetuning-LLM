from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "London", "New York"]) | F.col("dept_name").isin(["Engineering", "Sales"]))
)

location_summary = (
    valid_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4113000, "high").when(F.col("budget") >= 884000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)

result = (
    location_summary
    .filter(F.col("budget_count") >= 10)
    .select("location", "budget_bucket", "budget_count")
    .orderBy(F.col("budget_count").desc(), F.col("budget_bucket").desc(), "location")
    .limit(3)
)
