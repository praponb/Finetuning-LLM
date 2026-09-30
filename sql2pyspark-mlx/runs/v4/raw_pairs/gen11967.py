from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") == "London"))
    .groupBy("location")
    .agg(
        F.count("*").alias("num_departments"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("num_departments") > 3)
)
