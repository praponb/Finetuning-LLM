from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("element_length").asc_nulls_first())
w2 = Window.orderBy("element_length")
w_hist = Window.partitionBy("element").orderBy(F.col("element_length_cume_dist").desc_nulls_last())
w_running = Window.orderBy(F.col("element_length").desc_nulls_first())
w5 = Window.partitionBy("element")

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "created_at",
        "key",
        "v",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

base = (
    lines
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & (F.col("element") == "VIP"))
    .select(
        "id",
        "v",
        "element",
        F.upper("key").alias("key_upper"),
        F.concat_ws("-", "element", "key").alias("element_key_key"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

filtered = (
    base
    .filter(F.col("element").isNotNull())
    .select("*", F.length("element").alias("element_length"))
)

with_windows = (
    filtered
    .filter(F.col("element_length").between(21, 47))
    .select(
        "*",
        F.cume_dist().over(w).alias("element_length_cume_dist"),
        F.dense_rank().over(w2).alias("element_length_dense_rank"),
    )
)

filtered_carts = (
    with_windows
    .filter(F.col("element").isNotNull())
    .select(
        "id",
        "v",
        "element",
        "key_upper",
        "element_key_key",
        "key_clean",
        "element_length",
        "element_length_cume_dist",
        F.substring("element", 1, 2).alias("element_prefix"),
        F.when(F.col("element_length_cume_dist") >= 0.62, "High").when(F.col("element_length_cume_dist") >= 0.22, "Medium").otherwise("Low").alias("element_length_cume_dist_tier"),
    )
)

with_windows_carts = (
    filtered_carts
    .withColumn("element_length_cume_dist_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("element_length_dense_rank", F.dense_rank().over(w_running))
    .withColumn("element_max_element_length_cume_dist", F.max("element_length_cume_dist").over(w5))
)

result = (
    with_windows_carts
    .select(
        "id",
        "element",
        "key_upper",
        "element_key_key",
        "element_prefix",
        "element_length_cume_dist_tier",
        "element_length_cume_dist_cume_dist",
        "element_length_dense_rank",
    )
    .orderBy(F.col("element_length_cume_dist_cume_dist").asc_nulls_first())
)
