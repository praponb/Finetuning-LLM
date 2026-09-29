from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id", "key").orderBy(F.col("is_weight_kg").desc_nulls_last())
w_hist = Window.partitionBy("value_2", "key")
w4 = Window.partitionBy("key").orderBy("is_weight_kg")
w5 = Window.partitionBy("key").orderBy("value_2_avg_is_weight_kg")
w6 = Window.partitionBy("is_weight_kg_dense_rank_tier").orderBy(F.col("gift_events").desc())

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "customer_id",
        "key",
        "value",
        F.explode("coupon_codes").alias("value_2"),
    )
)

valid = (
    lines
    .filter(F.col("value_2") == "BUNDLE")
    .select(
        "id",
        "customer_id",
        "key",
        "value",
        "value_2",
        F.when(F.col("key") == "weight_kg", 1).otherwise(0).alias("is_weight_kg"),
        F.initcap(F.trim("value_2")).alias("value_2_title"),
        F.coalesce("value_2", F.lit("OTHER")).alias("resolved_value_2"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "key",
        "is_weight_kg",
        F.sum("is_weight_kg").over(w).alias("customer_id_sum_is_weight_kg"),
        F.percent_rank().over(w2).alias("is_weight_kg_percent_rank"),
        F.avg("is_weight_kg").over(w_hist).alias("value_2_avg_is_weight_kg"),
    )
)

metrics_carts = (
    metrics
    .filter(F.col("customer_id_sum_is_weight_kg") < 246)
    .select(
        "*",
        F.dense_rank().over(w4).alias("is_weight_kg_dense_rank"),
        F.dense_rank().over(w5).alias("value_2_avg_is_weight_kg_dense_rank"),
    )
)

totals = (
    metrics_carts
    .groupBy(
        F.when(F.col("is_weight_kg_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("is_weight_kg_dense_rank_tier"),
        F.when(F.col("value_2_avg_is_weight_kg_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("value_2_avg_is_weight_kg_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.count_if(F.col("key") == "gift").alias("gift_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.sort_array(F.collect_set("key")).alias("key_list_2"),
    )
    .withColumn("gift_events_dense_rank", F.dense_rank().over(w6))
)

result = (
    totals
    .select(
        "is_weight_kg_dense_rank_tier",
        "value_2_avg_is_weight_kg_dense_rank_tier",
        "gift_rows",
        "key_list",
        "gift_events_dense_rank",
    )
    .orderBy(
        F.col("gift_rows").asc_nulls_first(),
        F.col("value_2_avg_is_weight_kg_dense_rank_tier").desc(),
    )
)
