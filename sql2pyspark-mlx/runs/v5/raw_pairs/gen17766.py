from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_channel").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-15").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.when(F.col("amount") >= 1530, "High").when(F.col("amount") >= 380, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.paid_at",
        "e.amount_level",
        "e.method_filled",
        F.col("o.order_date"),
        F.col("o.channel"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.channel").alias("order_channel"))
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

totals = (
    sessions_payments
    .rollup(
        "order_channel",
        "channel",
        F.when(F.col("amount") >= 1770, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("paid_at").alias("ended_at"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.max(F.when(F.col("channel") == "store", F.col("burst_id"))).alias("store_max_burst_id"),
        F.round(F.avg("amount"), 2).alias("avg_amount_3"),
    )
    .filter(F.col("num_channels") > 1)
)

result = (
    totals
    .select(
        "channel",
        "amount_tier",
        "avg_amount",
        "num_channels",
        "avg_amount_2",
        "store_max_burst_id",
        "avg_amount_3",
    )
)
