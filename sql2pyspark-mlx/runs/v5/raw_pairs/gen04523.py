from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "Legal") & (F.col("budget") > 3740000))
    .groupBy(
        F.when(F.col("budget") >= 3952000, "high").when(F.col("budget") >= 1614000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
)
