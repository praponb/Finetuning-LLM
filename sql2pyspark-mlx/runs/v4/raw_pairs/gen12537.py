from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & (F.col("budget") >= 1218000) & (F.col("budget") >= 1932000))
    .groupBy("location")
    .agg(
        F.max("budget").alias("max_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("max_budget") >= 2182000)
    .orderBy(F.col("location").desc())
)
