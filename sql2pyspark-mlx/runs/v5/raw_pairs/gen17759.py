from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("customer_id", "promo").orderBy(F.col("product_id").desc_nulls_last())
w3 = Window.partitionBy("promo", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

with_windows = (
    flattened
    .filter(F.col("customer_id") == 31)
    .withColumn("promo_stddev_samp_price", F.stddev_samp("price").over(w))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w2))
    .withColumn("product_id_count_1h", F.count("product_id").over(w3))
)

valid = (
    with_windows
    .filter((F.col("product_id_cume_dist") >= 0.27) & (F.col("created_at") > F.lit("2025-03-01 15:00:00").cast("timestamp")))
    .select(
        "*",
        F.trim("promo").alias("clean_promo"),
        F.round("product_id_cume_dist", 0).alias("product_id_cume_dist_rounded"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.customer_id",
        "a.created_at",
        "a.coupon_codes",
        "a.price",
        "a.promo_stddev_samp_price",
        "a.product_id_cume_dist",
        "a.product_id_count_1h",
        "a.product_id_cume_dist_rounded",
        F.col("d.email"),
        F.col("d.country"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("a.*", F.col("d.age"))
)

enriched_carts = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.email").alias("customer_email"))
)

filtered = (
    enriched_carts
    .filter(F.col("created_at") >= F.lit("2025-03-01 22:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("country", ["TH", "JP"])
    .agg(F.max("is_us").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
