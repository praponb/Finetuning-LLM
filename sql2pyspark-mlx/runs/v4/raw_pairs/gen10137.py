from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3806000)
    .groupBy(F.when(F.col("budget") >= 4318000, "high").when(F.col("budget") >= 764000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Finance", "Engineering", "Marketing", "HR"])
    .agg(F.avg("budget"))
    .orderBy("budget_bucket")
)
