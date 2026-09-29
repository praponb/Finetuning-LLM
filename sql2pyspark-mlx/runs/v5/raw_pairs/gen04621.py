from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Engineering", "Finance"]))
    .groupBy(F.when(F.col("budget") >= 3069000, "high").when(F.col("budget") >= 1084000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Engineering", "Finance", "HR", "Legal"])
    .agg(F.count("budget"))
)
