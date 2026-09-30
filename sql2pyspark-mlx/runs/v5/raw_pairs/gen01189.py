from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy(F.col("order_amount").desc_nulls_last())
w2 = Window.orderBy(F.col("amount").asc_nulls_first())
w3 = Window.partitionBy("order_channel")

kept_keys = (
    df_shipments
    .select("id")
    .intersect(
        df_shipments
        .filter(F.col("carrier") == "Kerry")
        .select("id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("shipped_date") > F.lit("2023-07-16").cast("date")) & (F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
    .withColumn("cost_whole", F.ceil("cost"))
)

enriched = (
    base
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "left")
    .select("src.*", F.col("o.order_date"), F.col("o.amount"))
)

enriched_shipments = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.channel"),
        F.col("d.shipping_fee"),
        F.col("d.amount").alias("order_amount"),
    )
)

joined = (
    enriched_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.customer_id").isin([1, 7, 18, 20]))
    .select(
        "s.*",
        F.col("d.channel").alias("order_channel"),
        F.col("d.amount").alias("order_amount_2"),
        F.col("d.status").alias("order_status"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.cume_dist().over(w).alias("order_amount_cume_dist"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
        F.avg("order_amount_2").over(w3).alias("order_channel_avg_order_amount_2"),
    )
)

matched = (
    scored
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.delivered_date") >= F.col("pr.starts_on")) & (F.col("a.delivered_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .groupBy(F.date_trunc("week", "shipped_date").alias("week_start"))
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("order_channel_avg_order_amount_2", [0.25, 0.5, 0.75]).alias("order_channel_avg_order_amount_2_quartiles"),
        F.countDistinct("order_status").alias("distinct_order_status"),
    )
    .orderBy(F.col("week_start").asc_nulls_last())
)
