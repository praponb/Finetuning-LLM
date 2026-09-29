from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("is_jp").desc())

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

cleaned = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp")) & F.col("tag").isNotNull())
    .select("*", F.col("created_at").cast("date").alias("created_at_day"))
)

remaining_keys = (
    cleaned
    .select("id", "customer_id")
    .intersectAll(
        df_carts
        .filter(F.hour("created_at").between(12, 15))
        .select("id", "customer_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.segment"), F.col("p.country"), F.col("p.email"))
)

base = (
    enriched
    .filter(F.col("customer_id").isNotNull() & F.col("email").like("%@company.co"))
    .select(
        "customer_id",
        "created_at",
        "tag",
        "segment",
        "country",
        "email",
        F.concat_ws("-", "segment", "country").alias("segment_country_key"),
        F.when(F.col("country") == "JP", 1).otherwise(0).alias("is_jp"),
    )
)

kept_keys = (
    base
    .select("customer_id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id") == 31)
        .select("customer_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.stddev_samp("is_jp").over(w).alias("customer_id_stddev_samp_is_jp"),
        F.sum("is_jp").over(w2).alias("is_jp_sum_30m"),
        F.percent_rank().over(w3).alias("is_jp_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "customer_id",
        "created_at",
        "tag",
        "segment",
        "email",
        "is_jp",
        "customer_id_stddev_samp_is_jp",
        "is_jp_sum_30m",
        "is_jp_percent_rank",
    )
)
