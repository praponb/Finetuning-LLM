from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("is_welcome5").desc_nulls_last())
w4 = Window.partitionBy("code")
w5 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.partitionBy("code").orderBy("is_welcome5")
w7 = Window.orderBy(F.col("is_welcome5_dense_rank").asc_nulls_last())

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

base = (
    flattened
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("code").isNotNull() & (F.col("code") == "VIP"))
    .select(
        "*",
        F.when(F.col("code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

enriched = (
    base
    .withColumn("is_welcome5_count_15m", F.count("is_welcome5").over(w))
    .withColumn("is_welcome5_max_2h", F.max("is_welcome5").over(w2))
    .withColumn("is_welcome5_dense_rank", F.dense_rank().over(w3))
)

with_windows = (
    enriched
    .select(
        "*",
        F.count("is_welcome5_dense_rank").over(w4).alias("code_count_is_welcome5_dense_rank"),
        F.min("is_welcome5_dense_rank").over(w4).alias("code_min_is_welcome5_dense_rank"),
        F.avg("is_welcome5").over(w5).alias("is_welcome5_avg_1h"),
        F.dense_rank().over(w6).alias("is_welcome5_dense_rank_2"),
        F.cume_dist().over(w7).alias("is_welcome5_dense_rank_cume_dist"),
    )
)

base_carts = (
    with_windows
    .filter(F.col("created_at") < "2023-05-26")
    .withColumn("is_welcome5_avg_1h_per_is_welcome5_dense_rank_2", F.round((F.col("is_welcome5_avg_1h") / F.col("is_welcome5_dense_rank_2")), 2))
    .withColumn("created_at_weekofyear", F.weekofyear("created_at"))
)

valid = (
    base_carts
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("is_welcome5_count_15m").between(9, 11))
    .select(
        "*",
        F.greatest("is_welcome5_count_15m", "code_min_is_welcome5_dense_rank").alias("greatest_is_welcome5_count_15m_code_min_is_welcome5_dense_rank"),
        F.col("is_welcome5_max_2h").cast("string").alias("is_welcome5_max_2h_num"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("hour", "created_at").alias("hour_window"),
        F.when(F.col("is_welcome5_max_2h") >= 63, "LARGE").otherwise("SMALL").alias("is_welcome5_max_2h_tier"),
    )
    .agg(
        F.round(F.sum("code_count_is_welcome5_dense_rank") / F.nullif(F.count(F.when(F.col("code") == "VIP", 1)), F.lit(0)), 2).alias("code_count_is_welcome5_dense_rank_per_vip"),
        F.max("created_at").alias("ended_at"),
        F.count("*").alias("row_count"),
    )
)
