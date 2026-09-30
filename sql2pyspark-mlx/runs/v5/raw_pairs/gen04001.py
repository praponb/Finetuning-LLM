from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_last())

latest = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"), "order_id")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("method") == "bank_transfer", F.col("amount"))).alias("bank_transfer_max_amount"),
    )
)
