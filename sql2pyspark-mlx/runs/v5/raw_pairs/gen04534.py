from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(1080, 1430))
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
    )
    .orderBy(F.col("order_quarter").desc(), F.col("pending_discount").desc())
)
