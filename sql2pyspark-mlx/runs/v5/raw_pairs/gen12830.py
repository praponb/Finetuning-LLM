from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 30)
    .select("id", "order_id", "cost", F.length("status").alias("status_length"))
)
