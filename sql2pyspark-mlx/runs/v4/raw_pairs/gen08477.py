from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("paid_at") >= "2023-10-18") & (F.col("paid_at") < "2023-08-04") & (F.col("order_id") == 27)))
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("row_count"),
    )
    .orderBy("row_count")
)
