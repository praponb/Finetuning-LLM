from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age")).otherwise(0)).alias("corporate_age"),
    )
    .filter(F.col("corporate_age") < 71)
    .drop("corporate_age")
)
