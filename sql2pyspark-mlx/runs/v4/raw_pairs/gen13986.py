from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Sales"]) & (F.col("budget") >= 3480000))
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") < 19663000)
    .orderBy(F.col("location").desc())
)
