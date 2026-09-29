from pyspark.sql import functions as F

result = (
    df_customers
    .agg(F.count(F.when(F.col("city") == "London", 1)).alias("london_count"))
)
