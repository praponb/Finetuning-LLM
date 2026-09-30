from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Engineering", "Sales"]))
    .groupBy(
        F.when(F.col("budget") >= 3334000, "high").when(F.col("budget") >= 1976000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("budget_rank", F.rank().over(w))
)
