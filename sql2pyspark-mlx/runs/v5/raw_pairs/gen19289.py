from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount").between(870, 910) & (F.col("paid_at") >= "2023-04-24")) | F.col("status").isin(["SUCCESS", "FAILED"]))
    .select("method", "paid_at", F.hour("paid_at").alias("paid_at_hour"))
    .orderBy("method", "paid_at_hour")
)
