from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_total_budget"),
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("budget_per_bangkok"),
    )
    .filter(F.col("sales_events") >= 1)
    .orderBy(F.col("singapore_events").asc_nulls_last())
)
