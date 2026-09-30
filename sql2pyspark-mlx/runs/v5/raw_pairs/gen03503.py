from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy(F.col("order_amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("status")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

enriched = (
    df_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select(
        "a.*",
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.order_date"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "channel",
        "order_amount",
        "rank_in_group",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

scored = (
    islands
    .select(
        "*",
        F.sum("burst_id").over(w4).alias("burst_id_sum_7d"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
    )
)

matched = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.paid_at") >= F.col("pr.starts_on")) & (F.col("e.paid_at") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "channel",
        "rank_in_group",
        "burst_id",
        "burst_id_sum_7d",
        F.when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_B").otherwise("EXTENDED").alias("channel_zone"),
        F.col("max_shipment_cost"),
    )
)
