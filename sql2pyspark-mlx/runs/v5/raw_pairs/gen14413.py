from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("carrier")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("order_status").orderBy(F.col("order_id_min_cost").desc_nulls_first())

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["LOST", "DELIVERED"]), "Tier 1").otherwise("Other").alias("status_group"),
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

kept_keys = (
    cleaned
    .select("carrier", "id")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date") <= "2023-06-01")
        .select("carrier", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "status_group",
        "delivered_date_month_end",
        F.rank().over(w).alias("cost_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.min("cost").over(w3).alias("order_id_min_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share_2"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.shipped_date",
        "s.delivered_date",
        "s.status",
        "s.status_group",
        "s.order_id_min_cost",
        F.col("d.status").alias("order_status"),
        F.col("d.channel"),
        F.col("d.amount"),
    )
)

metrics = (
    enriched
    .filter(F.year("delivered_date") >= 2025)
    .select(
        "*",
        F.count("order_id_min_cost").over(w5).alias("channel_count_order_id_min_cost"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
        F.dense_rank().over(w6).alias("order_id_min_cost_dense_rank"),
    )
)

daily = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.avg("channel_count_order_id_min_cost").alias("avg_channel_count_order_id_min_cost"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

result = (
    daily
    .select(
        "quarter_start",
        "row_count",
        "avg_channel_count_order_id_min_cost",
        "amount_p90",
        "num_status",
        "distinct_channels",
    )
    .orderBy(F.col("quarter_start").desc())
)
