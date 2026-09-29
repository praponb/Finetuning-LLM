from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3165000, "high").when(F.col("budget") >= 1100000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .filter(F.col("unique_dept_names") >= 7)
    .orderBy("location", "budget_bucket")
    .limit(10)
)
