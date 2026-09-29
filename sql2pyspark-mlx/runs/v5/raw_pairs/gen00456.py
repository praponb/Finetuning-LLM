from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "code")
w2 = Window.partitionBy("code")
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy(F.col("customer_id_min_qty").desc_nulls_last())
w5 = Window.partitionBy("promo")
w6 = Window.orderBy("qty_share")

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

flattened = lines.select("customer_id", "qty", "promo", F.explode("coupon_codes").alias("code"))

with_windows = (
    flattened
    .select(
        "promo",
        "code",
        F.min("qty").over(w).alias("customer_id_min_qty"),
        F.max("qty").over(w2).alias("code_max_qty"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share"),
    )
)

base = (
    with_windows
    .filter(F.col("code").isNotNull() & (F.col("customer_id_min_qty") <= 3))
    .withColumn("code_max_qty_tier", F.when(F.col("code_max_qty") >= 5, "High").when(F.col("code_max_qty") >= 2, "Medium").otherwise("Low"))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("OTHER")))
)

scored = (
    base
    .select(
        "promo",
        "code",
        "code_max_qty",
        "code_max_qty_tier",
        "promo_clean",
        F.percent_rank().over(w4).alias("customer_id_min_qty_percent_rank"),
        F.max("customer_id_min_qty").over(w5).alias("promo_max_customer_id_min_qty"),
        F.rank().over(w6).alias("qty_share_rank"),
    )
)

daily = (
    scored
    .groupBy(
        F.when(F.col("promo_max_customer_id_min_qty") >= 4, "LARGE").otherwise("SMALL").alias("promo_max_customer_id_min_qty_tier"),
    )
    .agg(
        F.sum(F.when(F.col("code") == "VIP", F.col("promo_max_customer_id_min_qty")).otherwise(0)).alias("vip_promo_max_customer_id_min_qty"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("cnt") > 1)
)

result = (
    daily
    .select("promo_max_customer_id_min_qty_tier", "cnt", "event_count")
    .orderBy(F.col("promo_max_customer_id_min_qty_tier").asc_nulls_first())
)
