from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 4407000, "high").when(F.col("budget") >= 1895000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Finance", "Sales", "Engineering", "Marketing"])
    .agg(F.count("budget"))
)
