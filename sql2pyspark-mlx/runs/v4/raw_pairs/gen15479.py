from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value_2").orderBy(F.col("days_since_created_at").desc())
w2 = Window.partitionBy("key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("key", "value_2")
w_grp = Window.partitionBy("key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.partitionBy("value_2").orderBy(F.col("days_since_created_at_dense_rank").asc_nulls_first())
w7 = Window.orderBy(F.col("days_since_created_at").desc())
w_part_2 = Window.partitionBy("key").orderBy(F.col("days_since_created_at_dense_rank").desc_nulls_first())
w9 = Window.partitionBy("value_2", "key").orderBy(F.col("key_sum_days_since_created_at").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "created_at",
        "key",
        "value",
        F.explode("coupon_codes").alias("value_2"),
    )
)

base = (
    flattened
    .filter(F.col("key").isNotNull())
    .withColumn("value_2_lower", F.lower("value_2"))
)

filtered = (
    base
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.hour("created_at").between(7, 13))
    .select(
        "id",
        "created_at",
        "key",
        "value",
        "value_2",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("value_2", F.lit("UNKNOWN")).alias("resolved_value_2"),
    )
)

cleaned = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("value_2").isNotNull())
    .withColumn("value_2_upper", F.upper("value_2"))
    .withColumn("value_2_upper_2", F.upper("value_2"))
    .withColumn("value_2_filled", F.coalesce("value_2", F.lit("OTHER")))
)

enriched = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("days_since_created_at_dense_rank"),
        F.sum("days_since_created_at").over(w2).alias("days_since_created_at_sum_2h"),
        F.min("days_since_created_at").over(w3).alias("key_min_days_since_created_at"),
        F.sum("days_since_created_at").over(w4).alias("key_sum_days_since_created_at"),
        F.avg("days_since_created_at").over(w_grp).alias("days_since_created_at_avg_15m"),
    )
)

enriched_carts = (
    enriched
    .select(
        "*",
        F.min("key_min_days_since_created_at").over(w3).alias("key_min_key_min_days_since_created_at"),
        F.percent_rank().over(w6).alias("days_since_created_at_dense_rank_percent_rank"),
        F.rank().over(w7).alias("days_since_created_at_rank"),
        F.percent_rank().over(w_part_2).alias("days_since_created_at_dense_rank_percent_rank_2"),
        F.cume_dist().over(w9).alias("key_sum_days_since_created_at_cume_dist"),
    )
)

result = (
    enriched_carts
    .select(
        "id",
        "key",
        "value",
        "value_2",
        "days_since_created_at",
        "value_2_upper",
        "value_2_upper_2",
        "value_2_filled",
        "days_since_created_at_dense_rank",
    )
    .orderBy(F.col("value_2").desc_nulls_first())
)
