from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") | (F.col("location") != "Tokyo"))
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
    )
    .orderBy(F.col("min_budget").desc(), F.col("tokyo_budget").desc())
)
