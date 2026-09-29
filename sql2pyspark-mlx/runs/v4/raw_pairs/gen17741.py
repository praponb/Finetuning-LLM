from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("amount_tier").orderBy(F.col("returned_events").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1520, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("shipping_fee") >= 36, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.max(F.when(F.col("status") == "PENDING", F.col("amount"))).alias("pending_max_amount"),
        F.max(F.when(F.col("channel") == "mobile", F.col("amount"))).alias("mobile_max_amount"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.corr("amount", "discount").alias("corr_amount_discount"),
    )
    .filter(F.col("mobile_max_amount") > 0)
    .withColumn("returned_events_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("shipping_fee_tier").desc())
)
