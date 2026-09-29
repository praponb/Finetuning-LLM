from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "created_at", "coupon_codes", F.col("line.product_id"))
)

filtered = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-01 13:00:00").cast("timestamp")) & (F.col("created_at") < "2023-11-01"))
    .withColumn("product_id_str", F.col("product_id").cast("string"))
)

kept_keys = (
    filtered
    .select("id")
    .intersect(
        df_carts
        .filter(F.to_date("created_at") >= "2023-11-06")
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 18:00:00")))
    .select(
        "*",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.date_format("created_at", "yyyy-MM").alias("created_at_label"),
    )
)

filtered_carts = (
    base
    .filter(F.col("coupon_codes_count") < 27)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "product_id_str",
        "coupon_codes_count",
        "created_at_label",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.ceil("coupon_codes_count").alias("coupon_codes_count_whole"),
    )
)

result = (
    filtered_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id_str",
        "coupon_codes_count_whole",
    )
    .orderBy(F.col("coupon_codes_count_whole").desc(), "id")
)
