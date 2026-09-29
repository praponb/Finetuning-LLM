from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 710) & (F.col("customer_id").isNotNull() | (F.col("amount") >= 590)))
    .groupBy("status")
    .agg(
        F.countDistinct("amount").alias("unique_channels"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .orderBy(F.col("unique_channels").desc(), "status")
)
