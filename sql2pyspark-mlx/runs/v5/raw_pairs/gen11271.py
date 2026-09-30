from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("paid_at")

deduped = (
    df_payments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 1540, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("paid_at").alias("last_paid_at"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("week_start").desc())
)
