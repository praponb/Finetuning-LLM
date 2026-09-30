from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("paid_at").desc(), "id")

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.paid_at",
        F.col("o.amount").alias("order_amount"),
        F.col("o.channel"),
    )
)

filtered = (
    joined
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-11-12 04:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "paid_at",
        "channel",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "method_filled", "method_clean")
)

cleaned = (
    deduped
    .filter(F.col("channel").isNotNull())
    .withColumn("channel_upper", F.upper("channel"))
)

base = (
    cleaned
    .filter(F.col("channel") == "store")
    .withColumn("channel_group", F.when(F.col("channel").isin(["web", "mobile"]), "Priority").otherwise("Other"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("OTHER")))
)

daily = (
    base
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
    )
)

result = (
    daily
    .select("channel", "channel_list")
    .orderBy(F.col("channel").desc())
    .limit(10)
)
