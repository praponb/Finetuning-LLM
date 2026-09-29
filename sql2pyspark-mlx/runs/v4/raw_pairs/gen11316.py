from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_device").desc())
w2 = Window.orderBy("is_device", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("k", "v"))

filtered = (
    flattened
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select("*", F.substring("k", 1, 2).alias("k_prefix"))
)

valid = (
    filtered
    .filter(F.col("k").isin(["utm_source", "gift"]))
    .select(
        "id",
        "coupon_codes",
        "k",
        "v",
        "k_prefix",
        F.when(F.col("k") == "device", 1).otherwise(0).alias("is_device"),
    )
)

metrics = (
    valid
    .withColumn("is_device_dense_rank", F.dense_rank().over(w))
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w2))
)

result = (
    metrics
    .select(
        "id",
        "coupon_codes",
        "k",
        "v",
        "k_prefix",
        "is_device_dense_rank",
        "last_known_k",
    )
)
