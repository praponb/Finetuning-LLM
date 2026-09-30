from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3817000, "high").when(F.col("budget") >= 634000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy("budget_bucket")
)
