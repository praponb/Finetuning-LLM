from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "HR", "Engineering"]) | (F.col("budget") > 2384000))
    .groupBy(
        F.when(F.col("budget") >= 4228000, "high").when(F.col("budget") >= 1250000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.sum("budget").alias("total_budget"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)
