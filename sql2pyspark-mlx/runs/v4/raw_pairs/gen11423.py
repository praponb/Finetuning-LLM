from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_segment").orderBy(F.col("age_per_credit_limit").desc_nulls_last())
w2 = Window.partitionBy("customer_id")
w_hist = Window.partitionBy("customer_segment")
w_hist_2 = Window.orderBy(F.col("credit_limit").desc())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

valid = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & (F.col("customer_id") == 38))
    .select(
        "*",
        F.lower("tag").alias("tag_lower"),
        F.when(F.col("tag").isin(["SPRING10", "VIP"]), "Priority").otherwise("Other").alias("tag_group"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.credit_limit"), F.col("d.city"), F.col("d.segment"))
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.country"),
        F.col("p.age"),
        F.col("p.segment").alias("customer_segment"),
    )
)

filtered = (
    joined_carts
    .filter((F.col("created_at") > F.lit("2025-03-01 15:00:00").cast("timestamp")) & (F.col("city") != "Tokyo"))
    .select(
        "*",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
    )
)

result = (
    filtered
    .withColumn("age_per_credit_limit_rank", F.rank().over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
    .withColumn("customer_segment_min_credit_limit", F.min("credit_limit").over(w_hist))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w_hist_2))
)
