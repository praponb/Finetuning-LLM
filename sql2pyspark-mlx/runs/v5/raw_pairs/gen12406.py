from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("paid_at") >= "2023-06-22").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.discount"), F.col("o.amount"), F.col("o.channel"))
    .orderBy(F.col("id").desc(), "channel")
)
