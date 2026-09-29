from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "customer_id").orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "k")
w3 = Window.orderBy("dedup_rank_2", "id", "k")
w4 = Window.orderBy(F.col("dedup_rank_2").desc(), "id", "k")
w5 = Window.partitionBy("k").orderBy(F.col("seq_no").desc(), "id", "k")
w6 = Window.partitionBy("seq_no_bucket_tier").orderBy(F.col("event_seq_p50").desc())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

deduped = (
    lines
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "coupon_codes",
        "k",
        "v",
        "dedup_rank_2",
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows = (
    scored
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(3).over(w5).alias("seq_no_bucket"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.to_date("created_at") < "2025-01-01")
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("seq_no_bucket") >= 8, "LARGE").otherwise("SMALL").alias("seq_no_bucket_tier"),
        "seq_no",
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.countDistinct("k").alias("distinct_ks_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .withColumn("event_seq_p50_percent_rank", F.percent_rank().over(w6))
)
