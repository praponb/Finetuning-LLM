from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy("status", F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.min("paid_at").alias("first_paid_at"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
        F.max("amount").alias("max_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .filter(F.col("max_amount") > 0)
)
