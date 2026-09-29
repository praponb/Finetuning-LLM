from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .orderBy("status", "returned_count")
)
