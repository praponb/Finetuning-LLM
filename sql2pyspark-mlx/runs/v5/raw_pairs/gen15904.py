from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id", "promo_id")

base = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("amount").between(1130, 1180))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

cleaned = (
    base
    .filter((F.col("paid_at") >= F.lit("2023-09-03 20:00:00").cast("timestamp")) & F.col("amount").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status_prefix",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "amount", "paid_at", "amount_filled")
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    latest
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.amount") <= 1210)
    .select("s.*", F.col("d.channel"))
)

with_window = (
    enriched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.paid_at").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

deduped_payments = (
    with_window
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

summary = (
    deduped_payments
    .groupBy(
        F.when(F.col("amount") >= 1520, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("promo_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("amount_per_store"),
        F.percentile_approx("discount_pct", 0.9).alias("discount_pct_p90"),
    )
)

result = summary.select("amount_tier", "promo_id_tier", "amount_per_store")
