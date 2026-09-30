from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3040000, 3990000))
    .groupBy("dept_name")
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") >= 1638000)
    .orderBy("dept_name")
)
