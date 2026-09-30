from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("coupon_codes_count_rounded")
w2 = Window.orderBy("coupon_codes_count_rounded", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("coupon_codes_count", "id", "key")

lines = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("key", "value"))

base = (
    lines
    .filter(F.array_contains("coupon_codes", "VIP"))
    .withColumn("sorted_coupon_codes", F.sort_array("coupon_codes"))
    .withColumn("key_clean", F.coalesce("key", F.lit("DIRECT")))
)

filtered = (
    base
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "key",
        "value",
        "sorted_coupon_codes",
        F.size("coupon_codes").alias("coupon_codes_count"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("utm_source"))))
    .withColumn("coupon_codes_count_rounded", F.round("coupon_codes_count", 1))
    .withColumn("resolved_key", F.coalesce("key", F.lit("N/A")))
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "key", "coupon_codes_count", "coupon_codes_count_rounded")
)

result = (
    ranked
    .filter(F.col("coupon_codes_count") <= 58)
    .select(
        "*",
        F.first("coupon_codes_count_rounded", ignorenulls=True).over(w2).alias("first_known_coupon_codes_count_rounded"),
        F.ntile(10).over(w3).alias("coupon_codes_count_bucket"),
    )
)
