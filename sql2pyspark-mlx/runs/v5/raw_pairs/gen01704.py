from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "store") & (F.col("store_id").isin([28, 30]) | F.col("store_id").isin([5, 19, 31])))
    .orderBy("channel", F.col("amount").desc(), "id")
    .limit(10)
)
