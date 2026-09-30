from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("code").orderBy(F.col("code_length").desc_nulls_last())
w4 = Window.partitionBy("code").orderBy(F.col("code_length_max_15m").desc_nulls_last())
w_recent = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.orderBy(F.col("code_length_count_30m").desc_nulls_first())

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    flattened
    .filter(F.col("created_at") < "2023-06-03")
    .withColumn("code_prefix", F.substring("code", 1, 4))
    .withColumn("code_filled", F.coalesce("code", F.lit("N/A")))
)

base = (
    filtered
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("code").isNotNull())
    .select(
        "id",
        "created_at",
        "code",
        F.length("code").alias("code_length"),
        F.date_trunc("WEEK", "created_at").alias("created_at_period"),
    )
)

scored = (
    base
    .withColumn("code_avg_code_length", F.avg("code_length").over(w))
    .withColumn("code_length_max_15m", F.max("code_length").over(w2))
    .withColumn("code_length_dense_rank", F.dense_rank().over(w3))
    .withColumn("code_min_code_length", F.min("code_length").over(w))
)

with_windows = (
    scored
    .withColumn("code_length_max_15m_rank", F.rank().over(w4))
    .withColumn("code_length_count_30m", F.count("code_length").over(w_recent))
)

result = (
    with_windows
    .select(
        "*",
        F.round((F.col("code_length_max_15m_rank") / F.sum("code_length_max_15m_rank").over(w)), 4).alias("code_length_max_15m_rank_share"),
        F.percent_rank().over(w6).alias("code_length_count_30m_percent_rank"),
    )
)
