from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("promo_id").desc_nulls_last(), "id", "promo_id")
w2 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_whole").asc_nulls_last())
w3 = Window.partitionBy("channel", "promo_code")

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2024-04-01").cast("date")) & F.col("customer_id").isNotNull() & F.col("customer_id").isin([31, 32]))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

attributed = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

not_flagged = (
    attributed
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= F.col("s.order_date")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

valid = (
    latest
    .filter(F.col("order_date") > F.lit("2024-04-06").cast("date"))
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "clean_channel",
        "channel_filled",
        "resolved_channel",
        "promo_id",
        "promo_code",
        "discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.coalesce("promo_code", F.lit("UNKNOWN")).alias("resolved_promo_code"),
    )
)

scored = (
    valid
    .withColumn("discount_pct_whole_cume_dist", F.cume_dist().over(w2))
    .withColumn("channel_min_discount_pct_whole", F.min("discount_pct_whole").over(w3))
)

result = (
    scored
    .groupBy(
        F.when(F.col("promo_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
    )
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("promo_id")).otherwise(0)).alias("mobile_promo_id"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("distinct_promo_codes") >= 1)
)
