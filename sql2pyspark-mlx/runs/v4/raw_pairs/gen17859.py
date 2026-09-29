from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .orderBy("location")
)
