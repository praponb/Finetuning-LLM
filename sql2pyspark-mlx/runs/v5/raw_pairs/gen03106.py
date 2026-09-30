from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

flattened_carts = (
    flattened
    .select("id", "created_at", "promo", F.explode_outer("coupon_codes").alias("code"))
)

remaining_keys = (
    flattened_carts
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

reconciled = (
    flattened_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(~F.col("code").eqNullSafe(F.lit("BUNDLE")))
    .select("*", F.lower("code").alias("code_lower"))
)

result = (
    valid
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["activity_day", "promo"], ["activity_day"]],
        "activity_day",
        "promo",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count_if(F.col("promo") == "VIP").alias("vip_events"),
        F.grouping_id().alias("grouping_level"),
    )
)
