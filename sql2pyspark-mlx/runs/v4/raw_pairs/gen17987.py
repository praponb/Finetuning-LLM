from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
    .filter(F.col("avg_age") > 21)
)
