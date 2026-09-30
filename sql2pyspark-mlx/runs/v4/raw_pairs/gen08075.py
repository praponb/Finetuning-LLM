from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at"), F.col("c.amount").alias("payment_amount"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("amount") >= 1660, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.max("paid_at").alias("last_seen"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount"))).alias("shipped_total_amount"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount")).otherwise(0)).alias("store_discount"),
        F.min("paid_at").alias("first_seen"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("shipped_total_amount") > 0)
    .orderBy(F.col("activity_month").asc_nulls_last())
)
