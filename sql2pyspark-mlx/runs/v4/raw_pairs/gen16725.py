from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("channel") != "mobile")
    .select(
        "*",
        F.concat_ws("/", "promo_code", "channel").alias("promo_code_channel_key"),
        F.round((F.col("discount_pct") * 0.85), 2).alias("adjusted_discount_pct"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.last("adjusted_discount_pct", ignorenulls=True).over(w).alias("last_known_adjusted_discount_pct"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    ranked
    .filter((F.col("ends_on") > F.lit("2023-06-29").cast("date")) & F.col("channel").isNotNull())
    .withColumn("last_known_adjusted_discount_pct_band", F.when(F.col("last_known_adjusted_discount_pct") >= 69, "High").when(F.col("last_known_adjusted_discount_pct") >= 22, "Medium").otherwise("Low"))
    .withColumn("promo_code_upper", F.upper("promo_code"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

daily = (
    base
    .withColumn("week_start", F.date_trunc("week", "starts_on"))
    .withColumn("activity_month", F.date_trunc("month", "ends_on"))
    .rollup("week_start", "promo_code", "activity_month")
    .agg(
        F.sum("last_known_adjusted_discount_pct").alias("total_last_known_adjusted_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store"),
        F.grouping("week_start").alias("grouping_id"),
    )
)

result = (
    daily
    .select(
        "week_start",
        "promo_code",
        "channel_list",
        "store_events",
        "num_promo_codes",
        "discount_pct_per_store",
        F.when(F.col("promo_code").rlike("^(VIP|WELCOME5)$"), "GROUP_A").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "CORE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").asc_nulls_last(), "num_promo_codes")
)
