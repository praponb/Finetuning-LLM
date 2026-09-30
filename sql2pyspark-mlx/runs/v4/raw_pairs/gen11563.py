from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("credit_limit").desc_nulls_last())
w2 = Window.orderBy(F.col("credit_limit").desc_nulls_first())

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

joined = (
    lines
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.credit_limit"), F.col("p.country"), F.col("p.city"))
)

with_windows = (
    joined
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w2))
)

enriched = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.credit_limit").alias("customer_credit_limit"),
        F.col("p.email"),
        F.col("p.name"),
    )
)

remaining_keys = (
    enriched
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.size("coupon_codes") >= 2)
        .select("customer_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_carts = (
    verified
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.name").alias("customer_name"),
        F.col("b.city").alias("customer_city"),
        F.col("b.segment"),
    )
)

enriched_carts_2 = (
    enriched_carts
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.age"))
)

grouped = (
    enriched_carts_2
    .cube(
        "element",
        F.when(F.col("credit_limit_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("credit_limit_dense_rank_tier"),
    )
    .agg(
        F.min("credit_limit_dense_rank").alias("min_credit_limit_dense_rank"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("event_count"),
    )
)

result = (
    grouped
    .select("element", "event_count")
    .orderBy("element")
)
