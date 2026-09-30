from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("method").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("channel", "status").orderBy(F.col("seq_no").desc(), "id")
w5 = Window.partitionBy("amount_percent_rank_tier").orderBy(F.col("event_count").desc())

base = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("method").eqNullSafe(F.lit("bank_transfer"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.concat("status", F.lit(", "), "method").alias("status_method"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isNull())
    .select(
        "s.*",
        F.col("o.amount").alias("order_amount"),
        F.col("o.channel"),
        F.col("o.discount"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w).alias("amount_percent_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "status_method",
        "channel",
        "discount",
        "amount_percent_rank",
        "seq_no",
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(4).over(w4).alias("seq_no_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("amount_percent_rank") >= 0.6, "PREMIUM").otherwise("STANDARD").alias("amount_percent_rank_tier"),
        "seq_no",
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.count("*").alias("event_count"),
    )
    .withColumn("event_count_rank", F.rank().over(w5))
    .orderBy(F.col("amount_percent_rank_tier").asc_nulls_last())
)
