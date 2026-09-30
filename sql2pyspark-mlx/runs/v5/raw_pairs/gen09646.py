from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("method").orderBy(F.col("paid_at_hour").desc())
w_recent = Window.partitionBy("status", "channel").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

deduped = (
    df_payments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

valid = (
    deduped
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-08-28")))
    .withColumn("paid_at_hour", F.hour("paid_at"))
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.order_date"), F.col("b.channel"))
)

enriched_payments = (
    enriched
    .filter(F.col("paid_at_hour") == 36)
    .withColumn("order_id_min_paid_at_hour", F.min("paid_at_hour").over(w2))
    .withColumn("paid_at_hour_dense_rank", F.dense_rank().over(w3))
    .withColumn("paid_at_hour_sum_7d", F.sum("paid_at_hour").over(w_recent))
)

result = (
    enriched_payments
    .groupBy("method")
    .pivot("status", ["SUCCESS", "REFUNDED", "FAILED"])
    .agg(F.sum("amount").alias("amount"))
)
