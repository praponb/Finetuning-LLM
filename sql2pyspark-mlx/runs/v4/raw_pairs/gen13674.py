from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Berlin")
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3641000, "high").when(F.col("budget") >= 1166000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.count("*").alias("cnt"),
    )
)
