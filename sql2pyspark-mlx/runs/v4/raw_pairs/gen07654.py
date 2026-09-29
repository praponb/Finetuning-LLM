from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location") == "London")
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

summary = (
    base
    .groupBy("dept_name")
    .agg(F.count("*").alias("row_count"))
)

result = (
    summary
    .filter(F.col("row_count") >= 8)
    .orderBy(F.col("row_count").desc(), "dept_name")
    .limit(10)
)
