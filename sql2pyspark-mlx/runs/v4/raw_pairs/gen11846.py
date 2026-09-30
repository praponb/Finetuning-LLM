from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_ordered = Window.partitionBy("code")
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy(F.col("product_id_share_rounded").desc_nulls_first())
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_running = Window.orderBy(F.col("code_min_product_id_2").desc_nulls_last())

lines = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

lines_carts = (
    lines
    .select("created_at", "product_id", F.explode_outer("coupon_codes").alias("code"))
)

metrics = (
    lines_carts
    .select(
        "*",
        F.avg("product_id").over(w_part).alias("product_id_avg_2h"),
        F.min("product_id").over(w_ordered).alias("code_min_product_id"),
        F.round((F.col("product_id") / F.sum("product_id").over(w_ordered)), 4).alias("product_id_share"),
        F.count("product_id").over(w3).alias("product_id_count_15m"),
        F.max("product_id").over(w_ordered).alias("code_max_product_id"),
    )
)

cleaned = (
    metrics
    .filter(F.col("product_id_count_15m") == 13)
    .withColumn("product_id_share_rounded", F.round("product_id_share", 0))
)

scored = (
    cleaned
    .select(
        "*",
        F.count("product_id_count_15m").over(w4).alias("product_id_count_15m_count_1h"),
        F.round((F.col("product_id") / F.sum("product_id").over(w_ordered)), 4).alias("product_id_share_2"),
        F.round((F.col("code_min_product_id") / F.sum("code_min_product_id").over(w_ordered)), 4).alias("code_min_product_id_share"),
        F.stddev_samp("product_id_avg_2h").over(w_ordered).alias("code_stddev_samp_product_id_avg_2h"),
        F.min("product_id").over(w_ordered).alias("code_min_product_id_2"),
    )
)

valid = (
    scored
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("code_stddev_samp_product_id_avg_2h") > 5))
    .select(
        "created_at",
        "code_min_product_id",
        "product_id_share_rounded",
        "product_id_count_15m_count_1h",
        "product_id_share_2",
        "code_min_product_id_share",
        "code_stddev_samp_product_id_avg_2h",
        "code_min_product_id_2",
        F.coalesce("code_stddev_samp_product_id_avg_2h", F.lit(0)).alias("code_stddev_samp_product_id_avg_2h_filled"),
        F.col("product_id_share_2").cast("int").alias("product_id_share_2_int"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("code_clean"),
    )
)

valid_carts = (
    valid
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "created_at",
        "code_min_product_id",
        "product_id_share_rounded",
        "product_id_share_2",
        "code_min_product_id_share",
        "code_min_product_id_2",
        "code_stddev_samp_product_id_avg_2h_filled",
        "product_id_share_2_int",
        F.hour("created_at").alias("created_at_hour"),
    )
)

result = (
    valid_carts
    .select(
        "*",
        F.percent_rank().over(w5).alias("product_id_share_rounded_percent_rank"),
        F.count("product_id_share_2_int").over(w6).alias("product_id_share_2_int_count_1h"),
        F.sum("product_id_share_2_int").over(w7).alias("product_id_share_2_int_sum_30m"),
        F.rank().over(w_running).alias("code_min_product_id_2_rank"),
    )
)
