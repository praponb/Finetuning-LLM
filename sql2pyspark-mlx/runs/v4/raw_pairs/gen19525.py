from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
    )
    .filter(F.col("total_amount") >= 5000)
    .orderBy("total_amount", F.col("order_quarter").desc())
)
