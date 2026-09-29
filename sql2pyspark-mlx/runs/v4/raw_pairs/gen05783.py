from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("budget").between(3139000, 3521000) & (F.col("dept_name") != "Sales")))
    .groupBy("location")
    .agg(F.max("budget").alias("max_budget"))
    .orderBy("location", F.col("max_budget").desc())
)
