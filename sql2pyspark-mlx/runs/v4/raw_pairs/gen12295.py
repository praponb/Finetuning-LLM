from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.26) & (F.col("amount") > 1190) & F.col("shipping_fee").between(14, 25))
    .groupBy("store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
    .orderBy(F.col("pending_count").desc())
)
