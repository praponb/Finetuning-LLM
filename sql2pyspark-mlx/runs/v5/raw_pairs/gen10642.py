from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("qty").desc())

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

filtered = (
    exploded
    .filter(F.col("customer_id").isNotNull() & F.col("price").between(60, 130))
    .select(
        "customer_id",
        "coupon_codes",
        "qty",
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
    )
)

matched_keys = (
    filtered
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.size("coupon_codes") > 1)
        .select("customer_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.email"))
)

metrics = (
    enriched
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.rank().over(w2).alias("qty_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("qty") >= 2)
    .select(
        "customer_id",
        "coupon_codes",
        "qty",
        "is_welcome5",
        "email",
        "qty_rank",
        F.round("qty_share", 1).alias("qty_share_rounded"),
        F.size("coupon_codes").alias("coupon_codes_count"),
    )
)

daily = (
    valid
    .groupBy(
        F.when(F.col("coupon_codes_count") >= 79, "PREMIUM").otherwise("STANDARD").alias("coupon_codes_count_tier"),
        F.when(F.col("qty_share_rounded") >= 65, "LARGE").otherwise("SMALL").alias("qty_share_rounded_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("num_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
        F.max("coupon_codes_count").alias("max_coupon_codes_count"),
        F.count("*").alias("event_count"),
    )
)

result = (
    daily
    .select(
        "coupon_codes_count_tier",
        "qty_share_rounded_tier",
        "num_rows",
        "distinct_customer_ids_2",
        "max_coupon_codes_count",
        "event_count",
    )
    .orderBy(F.col("qty_share_rounded_tier").desc_nulls_first())
)
