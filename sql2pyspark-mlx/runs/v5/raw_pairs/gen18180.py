from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1493000, 3515000))
    .groupBy(
        F.when(F.col("budget") >= 4221000, "high").when(F.col("budget") >= 1603000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("pct_marketing") >= 63)
)
