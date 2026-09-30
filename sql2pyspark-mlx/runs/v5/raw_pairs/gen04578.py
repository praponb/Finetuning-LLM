from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4501000, "high").when(F.col("budget") >= 969000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("department_count"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("hr_budget") >= 46)
    .orderBy("department_count", "budget_bucket")
    .limit(5)
)
