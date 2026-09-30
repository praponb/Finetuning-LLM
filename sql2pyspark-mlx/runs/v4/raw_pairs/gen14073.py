from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("dept_name").isin(["Marketing", "Finance", "Engineering"]) & (F.col("dept_name") != "Sales")) | F.col("budget").between(2515000, 3817000))
    .withColumn("location_lower", F.lower("location"))
)

budget_bucket_summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3970000, "high").when(F.col("budget") >= 1036000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    budget_bucket_summary
    .select("budget_bucket", "location", "max_budget")
    .orderBy(F.col("max_budget").desc())
)
