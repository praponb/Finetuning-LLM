from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date")
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("store_id").orderBy(F.col("discount_p95").desc())

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    latest
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_latest = (
    attributed
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"))
)

result = (
    with_latest
    .groupBy(
        "store_id",
        "status",
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.avg(F.when(F.col("carrier") == "UPS", F.col("discount_pct"))).alias("ups_avg_discount_pct"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
    )
    .withColumn("discount_p95_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("store_id").desc_nulls_last())
)
