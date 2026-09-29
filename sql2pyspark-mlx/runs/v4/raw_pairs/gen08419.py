from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4346000, "high").when(F.col("budget") >= 1480000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("budget_count") > 3)
    .orderBy(F.col("budget_count").desc())
)
