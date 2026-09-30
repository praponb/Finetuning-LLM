from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("order_id").orderBy(F.col("amount_avg_3_prior").asc_nulls_first(), "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("amount_avg_3_prior").desc(), "id")

latest = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.paid_at",
        "s.latest_rank",
        F.col("o.shipping_fee"),
    )
)

metrics = (
    joined
    .withColumn("running_min_amount", F.min("amount").over(w2))
    .withColumn("running_avg_amount", F.avg("amount").over(w2))
    .withColumn("running_max_amount", F.max("amount").over(w2))
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w3))
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "shipping_fee",
        "running_min_amount",
        "running_max_amount",
        "amount_avg_3_prior",
    )
)

ranked = (
    deduped
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select("id", "order_id", "paid_at", "shipping_fee", "running_max_amount")
    .orderBy(F.col("order_id").desc())
)
