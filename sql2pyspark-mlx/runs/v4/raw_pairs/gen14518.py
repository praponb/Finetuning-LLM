from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())

deduped = (
    df_payments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy(F.date_trunc("month", "paid_at").alias("activity_month"))
    .agg(
        F.max(F.when(F.col("status") == "REFUNDED", F.col("amount"))).alias("refunded_max_amount"),
        F.min("amount").alias("min_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("amount_per_card"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
