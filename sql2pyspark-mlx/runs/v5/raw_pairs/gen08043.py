from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy(F.col("order_amount_2").desc_nulls_first())
w2 = Window.partitionBy("order_status")
w3 = Window.partitionBy("order_status", "order_id").orderBy(F.col("order_amount_2").asc_nulls_first())
w4 = Window.partitionBy("order_id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-16").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.when(F.col("amount") >= 1560, "High").when(F.col("amount") >= 420, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.amount",
        "a.status",
        "a.amount_band",
        F.col("d.status").alias("order_status"),
        F.col("d.amount").alias("order_amount"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.order_date") >= "2024-07-01")
    .select(
        "a.id",
        "a.order_id",
        "a.status",
        "a.amount_band",
        "a.order_status",
        F.col("p.amount").alias("order_amount_2"),
    )
)

matched_keys = (
    enriched
    .select("order_id")
    .exceptAll(
        df_payments
        .filter(F.col("amount") <= 1220)
        .select("order_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("order_amount_2_cume_dist", F.cume_dist().over(w))
    .withColumn("order_status_stddev_samp_order_amount_2", F.stddev_samp("order_amount_2").over(w2))
    .withColumn("order_amount_2_dense_rank", F.dense_rank().over(w3))
    .withColumn("order_id_avg_order_amount_2", F.avg("order_amount_2").over(w4))
)

result = (
    with_windows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["channel", "payment_status"], ["channel"], []],
        F.col("o.channel").alias("channel"),
        F.col("e.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("e.order_amount_2")).alias("avg_order_amount_2"),
        F.max(F.col("e.order_amount_2_dense_rank")).alias("max_order_amount_2_dense_rank"),
        F.grouping_id().alias("gid"),
    )
)
