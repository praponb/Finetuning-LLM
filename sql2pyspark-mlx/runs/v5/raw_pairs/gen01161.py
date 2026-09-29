from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3228000, "high").when(F.col("budget") >= 1404000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_dept_names"))
    .orderBy("budget_bucket")
)
