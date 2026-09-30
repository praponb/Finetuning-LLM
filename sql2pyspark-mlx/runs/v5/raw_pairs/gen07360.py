from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("key").orderBy(F.col("age").desc_nulls_first())
w4 = Window.partitionBy("customer_id").orderBy("age")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

matched_keys = (
    flattened
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2025-10-01")
        .select("customer_id", "id")
    )
)

verified = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("src.*", F.col("d.name"), F.col("d.credit_limit"), F.col("d.age"))
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").isNotNull())
    .select(
        "a.id",
        "a.customer_id",
        "a.coupon_codes",
        "a.key",
        "a.v",
        "a.age",
        F.col("c.name").alias("customer_name"),
    )
)

remaining_keys = (
    joined_carts
    .select("customer_id", "id")
    .intersectAll(
        df_carts
        .filter(F.col("created_at") < "2024-11-03")
        .select("customer_id", "id")
    )
)

reconciled = (
    joined_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "customer_id",
        "coupon_codes",
        "key",
        "v",
        "age",
        "customer_name",
        F.stddev_samp("age").over(w).alias("key_stddev_samp_age"),
        F.stddev_samp("age").over(w2).alias("customer_id_stddev_samp_age"),
        F.rank().over(w3).alias("age_rank"),
        F.percent_rank().over(w4).alias("age_percent_rank"),
    )
)
