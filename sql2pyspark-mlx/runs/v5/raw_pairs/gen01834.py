from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 1428000) & F.col("location").isin(["Singapore", "Berlin", "Bangkok"]))
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") >= 2028000)
    .drop("avg_budget")
    .orderBy("dept_name")
)
