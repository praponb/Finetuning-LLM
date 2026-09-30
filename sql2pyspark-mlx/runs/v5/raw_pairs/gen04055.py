from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id", "status")
w4 = Window.orderBy(F.col("amount").asc_nulls_last())
w5 = Window.partitionBy("status").orderBy("amount_dense_rank", "id")
w6 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w7 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("method") != "cash")
    .select(
        "*",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.avg("amount").over(w2).alias("amount_avg_3"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

ranked_payments = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    ranked_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "amount_avg_3",
        "amount_dense_rank",
        "rn",
        "rank_in_group",
        F.sum("new_group_flag").over(w7).alias("session_no"),
    )
)

flagged = (
    sessions_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w7))

kept_keys = (
    islands
    .select("status", "order_id")
    .subtract(
        df_payments
        .filter(F.col("amount").between(410, 460))
        .select("status", "order_id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.shipping_fee"), F.col("d.channel"))
)

result = (
    with_dims
    .groupBy("order_id", F.date_trunc("WEEK", "paid_at").alias("week_start"))
    .agg(
        F.avg("amount_avg_3").alias("avg_amount_avg_3"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("session_no_per_success"),
        F.min("amount_dense_rank").alias("min_amount_dense_rank"),
    )
    .filter(F.col("duration_min") < 1000000)
)
