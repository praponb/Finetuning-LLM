from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2339000)
    .groupBy(
        F.when(F.col("budget") >= 3177000, "high").when(F.col("budget") >= 1984000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") >= 74)
    .select("budget_bucket", "dept_name", "budget_range")
    .orderBy(F.col("budget_range").desc(), "dept_name")
)
