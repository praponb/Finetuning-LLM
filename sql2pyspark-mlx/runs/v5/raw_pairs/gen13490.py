from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4476000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names_2"),
    )
)
