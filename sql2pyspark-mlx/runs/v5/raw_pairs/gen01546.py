from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & (F.col("location") != "Bangkok"))
    .agg(F.min("budget").alias("min_budget"))
    .orderBy(F.col("min_budget").desc())
)
