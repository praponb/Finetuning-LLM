from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3206000)
    .groupBy(
        F.when(F.col("budget") >= 4344000, "high").when(F.col("budget") >= 1839000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"))
    .orderBy("location", "budget_bucket")
    .limit(1)
)
