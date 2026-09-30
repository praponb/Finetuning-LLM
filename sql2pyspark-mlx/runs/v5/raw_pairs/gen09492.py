from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2092000, 2495000) & ((F.col("budget") >= 1946000) | (F.col("dept_name") == "Legal")))
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
    .orderBy(F.col("avg_budget").desc())
)
