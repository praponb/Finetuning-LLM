from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("paid_at", "id")
w5 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("order_amount")

enriched = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.count("amount").over(w2).alias("amount_count_7d"),
        F.sum("amount").over(w3).alias("amount_sum_30d"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.channel"))
)

joined_payments = (
    joined
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.paid_at",
        "s.amount_sum_30d",
        "s.channel",
        F.col("d.discount"),
        F.col("d.amount").alias("order_amount"),
    )
)

with_prev = joined_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w4))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        "channel",
        "order_amount",
        F.sum("is_new_burst").over(w5).alias("session_no"),
    )
)

ranked = (
    islands
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

attributed = (
    ranked
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.paid_at") >= F.col("pr.starts_on")) & (F.col("e.paid_at") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

daily = (
    attributed
    .withColumn("order_amount_tier", F.when(F.col("order_amount") >= 1300, "PREMIUM").otherwise("STANDARD"))
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .groupingSets(
        [["order_amount_tier", "session_no_tier", "quarter_start"], ["order_amount_tier", "session_no_tier"], []],
        "order_amount_tier",
        "session_no_tier",
        "quarter_start",
    )
    .agg(
        F.round(F.avg("promo_id"), 2).alias("avg_promo_id"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("promo_id")).otherwise(0)).alias("welcome5_promo_id"),
    )
)

result = (
    daily
    .select(
        "order_amount_tier",
        "session_no_tier",
        "quarter_start",
        "avg_promo_id",
        "mobile_rows",
        "num_rows",
        "welcome5_promo_id",
    )
    .orderBy(F.col("avg_promo_id").desc_nulls_first())
)
