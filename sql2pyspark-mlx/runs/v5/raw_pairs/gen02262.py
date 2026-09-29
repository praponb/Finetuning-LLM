from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag", "key").orderBy(F.col("tag_length").desc_nulls_first())
w_recent = Window.partitionBy("key", "customer_id").orderBy(F.col("tag_length").desc_nulls_last())
w3 = Window.partitionBy("customer_id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

lines_carts = (
    lines
    .select("id", "customer_id", "key", F.explode_outer("coupon_codes").alias("tag"))
)

base = (
    lines_carts
    .filter(F.col("tag").isNotNull())
    .select("*", F.length("tag").alias("tag_length"))
)

enriched = (
    base
    .select(
        "customer_id",
        "key",
        "tag",
        "tag_length",
        F.dense_rank().over(w).alias("tag_length_dense_rank"),
        F.dense_rank().over(w_recent).alias("tag_length_dense_rank_2"),
        F.sum("tag_length").over(w3).alias("customer_id_sum_tag_length"),
    )
)

cleaned = (
    enriched
    .filter(F.col("tag_length_dense_rank").between(3, 5))
    .select(
        "*",
        F.upper("tag").alias("tag_upper"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

enriched_carts = (
    cleaned
    .filter(F.col("tag").isin(["WELCOME5", "SPRING10", "VIP"]))
    .select(
        "*",
        F.round((F.col("tag_length_dense_rank_2") / F.sum("tag_length_dense_rank_2").over(w3)), 4).alias("tag_length_dense_rank_2_share"),
    )
)

enriched_carts_2 = (
    enriched_carts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.credit_limit"), F.col("p.age"), F.col("p.segment"))
)

result = (
    enriched_carts_2
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "tag_length_dense_rank",
        "tag_length_dense_rank_2",
        "customer_id_sum_tag_length",
        "tag_upper",
        "key_filled",
        "tag_length_dense_rank_2_share",
        "age",
        "segment",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "OTHER_ZONE").when(F.col("segment").rlike("^(Consumer)$"), "EXTENDED").otherwise("GROUP_B").alias("segment_zone"),
        F.col("avg_order_shipping_fee"),
    )
    .orderBy(F.col("tag_length_dense_rank_2_share").desc_nulls_first())
)
