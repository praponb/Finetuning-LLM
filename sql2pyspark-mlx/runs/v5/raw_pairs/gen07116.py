from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 4289000, "high").when(F.col("budget") >= 759000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("dept_name", ["Finance", "Sales", "HR"])
    .agg(F.max("budget"))
)
