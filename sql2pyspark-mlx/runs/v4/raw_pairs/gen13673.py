from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("avg_amount") >= 570)
    .orderBy("total_amount")
)
