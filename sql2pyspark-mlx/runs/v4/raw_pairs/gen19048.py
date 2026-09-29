from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 3340000) | F.col("location").isNotNull())
    .groupBy("dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        F.count("*").alias("num_departments"),
    )
)
