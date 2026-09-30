from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4399000, "high").when(F.col("budget") >= 1933000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .filter(F.col("unique_dept_names") < 5)
    .select("budget_bucket", "location", "stddev_budget", "unique_dept_names")
    .orderBy("stddev_budget")
)
