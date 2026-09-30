from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location") == "Tokyo")

summary = (
    recent_departments
    .groupBy("location")
    .agg(F.max("budget").alias("max_budget"))
)

result = summary.filter(F.col("max_budget") > 2064000)
