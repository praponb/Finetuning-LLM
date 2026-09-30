from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.orderBy("promo_id", "id", "promo_id")
w3 = Window.partitionBy("method").orderBy(F.col("promo_id").asc_nulls_first())
w4 = Window.orderBy(F.col("amount").desc(), "id", "promo_id")
w5 = Window.partitionBy("status", "method")
w6 = Window.partitionBy("method", "status")

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.method"))
)

attributed = (
    with_latest
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

scored = (
    attributed
    .select(
        "*",
        F.ntile(4).over(w2).alias("promo_id_bucket"),
        F.rank().over(w3).alias("promo_id_rank"),
        F.row_number().over(w4).alias("seq_no"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w5)), 4).alias("promo_id_share"),
        F.stddev_samp("shipping_fee").over(w6).alias("method_stddev_samp_shipping_fee"),
    )
)

matched = (
    scored
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

stats = (
    matched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .withColumn("promo_id_share_tier", F.when(F.col("promo_id_share") >= 0.76, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "promo_id_share_tier"], ["quarter_start"], []],
        "quarter_start",
        "promo_id_share_tier",
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
    .filter(F.col("partner_count") > 1)
)

result = stats.select("promo_id_share_tier", "num_methods", "partner_count")
