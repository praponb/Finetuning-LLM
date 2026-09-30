from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3218000, 3714000))
    .groupBy(
        "dept_name",
        "location",
        F.when(F.col("budget") >= 3376000, "high").when(F.col("budget") >= 1590000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("min_budget") >= 3417000)
    .orderBy("budget_bucket", "location")
)
