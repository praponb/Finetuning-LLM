from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4150000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Singapore", 1)), F.lit(0)), 2).alias("budget_per_singapore"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
    )
)
