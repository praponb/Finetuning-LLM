from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_status").orderBy("amount", "id")
w3 = Window.partitionBy("order_id").orderBy("amount", "id")

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("*", F.concat_ws("-", "status", "method").alias("status_method_key"))
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.channel"), F.col("b.status").alias("order_status"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("amount_bucket", F.ntile(3).over(w3))
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status_method_key",
        "channel",
        "dedup_rank",
        "amount_bucket",
    )
    .orderBy(F.col("amount").desc())
)
