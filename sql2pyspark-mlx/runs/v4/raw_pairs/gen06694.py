from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("price").desc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.partitionBy("country")
w4 = Window.partitionBy("segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("price").asc_nulls_first())

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.percent_rank().over(w).alias("price_percent_rank"),
        F.sum("pos").over(w2).alias("pos_sum_1h"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.signup_date"), F.col("p.segment"), F.col("p.age"))
)

with_dims_carts = (
    with_dims
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select(
        "e.*",
        F.col("c.name"),
        F.col("c.signup_date").alias("customer_signup_date"),
        F.col("c.country"),
    )
)

metrics = (
    with_dims_carts
    .withColumn("country_min_pos_sum_1h", F.min("pos_sum_1h").over(w_grp))
    .withColumn("price_avg_15m", F.avg("price").over(w4))
    .withColumn("price_rank", F.rank().over(w5))
)

matched_keys = (
    metrics
    .select("customer_id")
    .intersectAll(
        df_carts
        .filter(F.hour("created_at").between(6, 13))
        .select("customer_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.credit_limit"),
        F.col("b.email"),
        F.col("b.age").alias("customer_age"),
    )
)

totals = (
    joined
    .groupBy("segment")
    .agg(
        F.avg("country_min_pos_sum_1h").alias("avg_country_min_pos_sum_1h"),
        F.avg("age").alias("avg_age"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
)

result = (
    totals
    .select(
        "segment",
        "avg_country_min_pos_sum_1h",
        "avg_age",
        "uk_count",
        F.when(F.col("segment").rlike("^(Consumer|Corporate)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").desc())
)
