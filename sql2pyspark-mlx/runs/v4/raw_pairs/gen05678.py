from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2395000)
    .groupBy("location")
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") > 1224000)
    .orderBy("location", "max_budget")
)
