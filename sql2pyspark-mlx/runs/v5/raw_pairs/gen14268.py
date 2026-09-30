from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("is_weight_kg", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("is_weight_kg").asc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("is_weight_kg").asc_nulls_last())
w4 = Window.partitionBy("key").orderBy(F.col("is_weight_kg").desc(), "id", "key")
w5 = Window.partitionBy("key").orderBy("is_weight_kg", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("cumulative_is_weight_kg_2", "id", "key").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("key", "customer_id").orderBy(F.col("is_weight_kg_percent_rank").asc_nulls_last())
w8 = Window.partitionBy("customer_id").orderBy(F.col("is_weight_kg_percent_rank").desc(), "id", "key")
w_hist = Window.partitionBy("key").orderBy("is_weight_kg_dense_rank", "id", "key")
w10 = Window.orderBy("row_seq", "id", "key")
w11 = Window.partitionBy("key").orderBy(F.col("next_is_weight_kg_percent_rank_percent_rank").desc_nulls_first())
w12 = Window.partitionBy("key").orderBy(F.col("prev_row_seq").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("key").isNotNull() & F.col("key").isin(["gift", "weight_kg", "device"]))
    .select(
        "*",
        F.when(F.col("key") == "weight_kg", 1).otherwise(0).alias("is_weight_kg"),
        F.coalesce("key", F.lit("Unknown")).alias("key_filled"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "customer_id",
        "key",
        "is_weight_kg",
        F.sum("is_weight_kg").over(w).alias("cumulative_is_weight_kg"),
        F.dense_rank().over(w2).alias("is_weight_kg_dense_rank"),
        F.percent_rank().over(w3).alias("is_weight_kg_percent_rank"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("is_weight_kg").over(w5).alias("cumulative_is_weight_kg_2"),
    )
)

with_windows = (
    metrics
    .filter(F.col("cumulative_is_weight_kg").between(357, 371))
    .select(
        "id",
        "key",
        "is_weight_kg",
        "cumulative_is_weight_kg",
        "is_weight_kg_dense_rank",
        F.avg("is_weight_kg_percent_rank").over(w_part).alias("is_weight_kg_percent_rank_avg_5_prior"),
        F.percent_rank().over(w_ordered).alias("is_weight_kg_percent_rank_percent_rank"),
        F.row_number().over(w8).alias("row_seq"),
        F.last("is_weight_kg", ignorenulls=True).over(w5).alias("last_known_is_weight_kg"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "key",
        "is_weight_kg",
        F.lag("row_seq").over(w_hist).alias("prev_row_seq"),
        F.lead("is_weight_kg_percent_rank_percent_rank").over(w_hist).alias("next_is_weight_kg_percent_rank_percent_rank"),
        F.lag("is_weight_kg_percent_rank_percent_rank").over(w_hist).alias("prev_is_weight_kg_percent_rank_percent_rank"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.lead("is_weight_kg", 2, 0).over(w10).alias("next_is_weight_kg"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w11))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("row_num", F.rank().over(w12))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "key",
        "next_is_weight_kg_percent_rank_percent_rank",
        "next_is_weight_kg",
        "rank_in_group",
    )
)

result = (
    ranked
    .cube("key")
    .agg(
        F.sum(F.when(F.col("key") == "weight_kg", F.col("next_is_weight_kg_percent_rank_percent_rank")).otherwise(0)).alias("weight_kg_next_is_weight_kg_percent_rank_percent_rank"),
        F.max(F.when(F.col("key") == "utm_source", F.col("next_is_weight_kg"))).alias("utm_source_max_next_is_weight_kg"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_rows"),
        F.sum(F.when(F.col("key") == "gift", F.col("next_is_weight_kg")).otherwise(0)).alias("gift_next_is_weight_kg"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.percentile_approx("next_is_weight_kg", 0.9).alias("next_is_weight_kg_p90"),
    )
    .orderBy(F.col("num_keys").desc_nulls_last())
)
