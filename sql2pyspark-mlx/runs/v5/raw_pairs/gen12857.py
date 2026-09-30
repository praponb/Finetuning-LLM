from pyspark.sql import functions as F

recent_departments = df_departments.filter(~F.col("dept_name").isin(["Finance", "Marketing"]))

departments_stats = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3614000, "high").when(F.col("budget") >= 624000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
)

result = departments_stats.select("budget_bucket", "unique_locations")
