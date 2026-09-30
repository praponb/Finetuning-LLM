from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("location").isin(["Berlin", "Bangkok", "Singapore"]) & (F.col("dept_name") == "Finance")) | F.col("budget").between(1614000, 1659000))
    .withColumn("adjusted_budget", F.round((F.col("budget") * 0.9), 2))
)

departments_stats = (
    valid_departments
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
)

result = departments_stats.select("location", "total_budget")
