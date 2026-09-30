from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 3549000, "high").when(F.col("budget") >= 984000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Engineering", "Sales"])
    .agg(F.count("budget"))
    .orderBy("budget_bucket")
)
