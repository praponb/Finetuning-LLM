from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3891000, "high").when(F.col("budget") >= 1104000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy(F.col("budget_range").desc())
)
