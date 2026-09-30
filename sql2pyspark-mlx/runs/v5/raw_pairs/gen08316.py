from pyspark.sql import functions as F

src = df_orders.filter(F.col("store_id") == 9)

channel_summary = (
    src
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = channel_summary.filter(F.col("cancelled_count") < 69)
