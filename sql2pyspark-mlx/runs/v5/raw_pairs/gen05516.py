from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("channel_length").asc_nulls_first(), "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("rn_2", "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("rn_plus_rn_2").desc_nulls_first(), "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isin(["store", "mobile"]))
    .select(
        "*",
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_promotions = (
    latest
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

base = (
    latest_promotions
    .filter((F.col("starts_on") > F.lit("2023-08-25").cast("date")) & F.col("discount_pct").between(0.18, 0.2))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "resolved_channel",
        "rn",
        "rn_2",
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
        F.concat_ws("/", "channel", "promo_code").alias("channel_promo_code_key"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

base_promotions = (
    deduped
    .filter(F.col("starts_on") >= F.to_date(F.lit("2023-07-22")))
    .withColumn("rn_plus_rn_2", (F.col("rn") + F.col("rn_2")))
    .withColumn("starts_on_plus_56d", F.date_add("starts_on", 56))
)

deduped_promotions = (
    base_promotions
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_promotions
    .groupBy(
        F.date_trunc("MONTH", "starts_on_plus_months").alias("activity_month"),
        F.when(F.col("rn_plus_rn_2") >= 84, "HIGH").otherwise("LOW").alias("rn_plus_rn_2_tier"),
    )
    .agg(
        F.max(F.when(F.col("channel") == "store", F.col("rn_plus_rn_2"))).alias("store_max_rn_plus_rn_2"),
        F.percentile_approx("rn_plus_rn_2", [0.25, 0.5, 0.75]).alias("rn_plus_rn_2_quartiles"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
)
