from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2023-03-01")
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 7)
    .orderBy("row_count")
)
