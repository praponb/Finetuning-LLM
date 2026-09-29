from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("dept_name") != "Finance") & (F.col("dept_name") == "Finance")) | (F.col("budget") >= 1259000))
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy("budget_range")
)
