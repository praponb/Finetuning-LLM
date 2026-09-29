from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget_bucket")

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .groupBy(
        F.when(F.col("budget") >= 4082000, "high").when(F.col("budget") >= 1915000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
    .orderBy(F.col("total_budget").desc())
)
