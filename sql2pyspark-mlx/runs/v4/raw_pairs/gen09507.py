from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([2, 14, 20]) & (F.col("amount") <= 1130))
    .select("id", "store_id", "channel", "status")
    .orderBy("channel")
)
