from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai"]))
    .withColumn("clean_location", F.trim("location"))
)

agg = (
    base
    .groupBy("dept_name")
    .agg(F.sum("budget").alias("total_budget"))
)

result = (
    agg
    .filter(F.col("total_budget") >= 11358000)
    .select("dept_name", "total_budget")
)
