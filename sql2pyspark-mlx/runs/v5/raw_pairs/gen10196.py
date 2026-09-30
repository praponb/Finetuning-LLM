from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2226000)
    .agg(
        F.max("budget").alias("max_budget"),
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("num_departments"),
    )
    .orderBy("max_budget")
)
