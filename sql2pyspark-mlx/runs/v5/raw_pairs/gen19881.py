from pyspark.sql import functions as F

result = (
    df_departments
    .rollup(
        "dept_name",
        F.when(F.col("budget") >= 3515000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg(F.when(F.col("dept_name") == "Engineering", F.col("budget"))).alias("engineering_avg_budget"),
    )
    .orderBy(F.col("budget_tier").asc_nulls_last())
)
