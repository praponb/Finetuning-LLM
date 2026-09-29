from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3336000, "high").when(F.col("budget") >= 1500000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("avg_budget") > 37)
    .select("budget_bucket", "location_list", "avg_budget")
)
