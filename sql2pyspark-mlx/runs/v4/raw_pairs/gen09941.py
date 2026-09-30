from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount", "channel", F.col("amount").cast("int").alias("amount_int"))
    .orderBy("channel")
)
