from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(6, 10))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
    )
    .orderBy("order_id")
)
