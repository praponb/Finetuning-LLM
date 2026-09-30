from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2021000)
    .groupBy(
        F.when(F.col("budget") >= 3571000, "high").when(F.col("budget") >= 1282000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
    )
    .filter(F.col("legal_budget") < 65)
    .select("budget_bucket", "location", "legal_budget")
    .orderBy("location", "budget_bucket")
    .limit(3)
)
