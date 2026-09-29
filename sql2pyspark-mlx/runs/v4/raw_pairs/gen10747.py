from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_departments
    .filter(F.col("budget") >= 1683000)
    .groupBy(
        F.when(F.col("budget") >= 4154000, "high").when(F.col("budget") >= 694000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("pct_of_total", F.round(((F.col("total_budget") * 100) / F.sum("total_budget").over(w)), 2))
)
