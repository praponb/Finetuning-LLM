from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

base = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull())
    .withColumn("price_band", F.when(F.col("price") >= 249, "High").when(F.col("price") >= 39, "Medium").otherwise("Low"))
    .withColumn("product_id_times_qty", (F.col("product_id") * F.col("qty")))
)

valid = (
    base
    .filter(F.col("promo").isNotNull())
    .select(
        "created_at",
        "product_id",
        "qty",
        "price",
        "promo",
        "price_band",
        "product_id_times_qty",
        (F.col("price") + F.col("product_id")).alias("price_plus_product_id"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

metrics = (
    valid
    .withColumn("promo_avg_qty", F.avg("qty").over(w))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
)

result = (
    metrics
    .select(
        "qty",
        "promo",
        "price_plus_product_id",
        "promo_avg_qty",
        F.when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "CORE").when(F.col("promo").rlike("^(SPRING10|VIP)$"), "PRIORITY").otherwise("GROUP_A").alias("promo_zone"),
    )
)
