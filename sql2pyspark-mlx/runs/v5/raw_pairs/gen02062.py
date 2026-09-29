from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age")
w2 = Window.partitionBy("customer_id", "segment").orderBy(F.col("age_dense_rank").asc_nulls_first())
w3 = Window.partitionBy("key")
w4 = Window.orderBy(F.col("age").desc())
w5 = Window.partitionBy("segment", "customer_id")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

cleaned = (
    lines
    .filter(F.col("key").isNotNull())
    .select("*", F.array_join("coupon_codes", "|").alias("coupon_codes_text"))
)

joined = (
    cleaned
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.signup_date") >= "2023-05-01")
    .select("a.*", F.col("d.age"), F.col("d.email"), F.col("d.segment"))
)

enriched = joined.withColumn("age_dense_rank", F.dense_rank().over(w))

enriched_carts = (
    enriched
    .select(
        "id",
        "customer_id",
        "key",
        "value",
        "email",
        "age_dense_rank",
        F.dense_rank().over(w2).alias("age_dense_rank_dense_rank"),
        F.max("age").over(w3).alias("key_max_age"),
        F.dense_rank().over(w4).alias("age_dense_rank_2"),
        F.round((F.col("age") / F.sum("age").over(w5)), 4).alias("age_share"),
    )
)

stats = (
    enriched_carts
    .groupBy(
        F.when(F.col("age_share") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("age_share_tier"),
        F.when(F.col("age_dense_rank_2") >= 6, "LARGE").otherwise("SMALL").alias("age_dense_rank_2_tier"),
    )
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count_if(F.col("key") == "device").alias("device_events"),
        F.max("age_dense_rank_2").alias("max_age_dense_rank_2"),
    )
    .filter(F.col("num_keys") >= 1)
)

result = (
    stats
    .select(
        "age_share_tier",
        "age_dense_rank_2_tier",
        "device_events",
        "max_age_dense_rank_2",
    )
    .orderBy(F.col("max_age_dense_rank_2").desc_nulls_first(), "age_dense_rank_2_tier")
)
