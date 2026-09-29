from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .orderBy("dept_name", F.col("london_budget").desc())
)
