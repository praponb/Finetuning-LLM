from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("promo").orderBy(F.col("is_vip_flag_2").asc_nulls_last())
w2 = Window.partitionBy("promo").orderBy(F.col("is_vip_flag").desc_nulls_first())
w_part = Window.orderBy("is_vip_flag_2")
w_part_2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_first())

lines = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

filtered = (
    lines
    .filter(F.col("created_at") >= F.lit("2025-03-01 11:00:00").cast("timestamp"))
    .select(
        "id",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        F.lower("promo").alias("promo_lower"),
        F.date_format("created_at", "yyyy-MM").alias("created_at_label"),
    )
)

base = (
    filtered
    .filter(F.col("qty") < 4)
    .select(
        "id",
        "coupon_codes",
        "price",
        "promo",
        "promo_lower",
        F.array_contains("coupon_codes", "VIP").alias("is_vip"),
        F.coalesce("promo", F.lit("DIRECT")).alias("resolved_promo"),
    )
)

cleaned = (
    base
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "*",
        F.col("is_vip").cast("int").alias("is_vip_flag"),
        F.col("is_vip").cast("int").alias("is_vip_flag_2"),
    )
)

enriched = (
    cleaned
    .withColumn("is_vip_flag_2_rank", F.rank().over(w_hist))
    .withColumn("is_vip_flag_cume_dist", F.cume_dist().over(w2))
    .withColumn("is_vip_flag_2_cume_dist", F.cume_dist().over(w_part))
    .withColumn("price_rank", F.rank().over(w_part_2))
)

result = (
    enriched
    .select(
        "id",
        "coupon_codes",
        "price",
        "promo",
        "promo_lower",
        "resolved_promo",
        "is_vip_flag",
        "is_vip_flag_2_rank",
        "price_rank",
        F.when(F.col("promo").rlike("^(BUNDLE|SPRING10)$"), "GROUP_B").when(F.col("promo").rlike("^(VIP|WELCOME5)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("promo_zone"),
    )
    .orderBy(F.col("is_vip_flag").asc_nulls_last())
)
