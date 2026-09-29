from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & (F.col("budget") > 1750000))
    .withColumn("budget_tier", F.when(F.col("budget") >= 3234000, "High").when(F.col("budget") >= 880000, "Medium").otherwise("Low"))
)

summary = (
    base
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
)

result = summary.filter(F.col("total_budget") > 8987000)
