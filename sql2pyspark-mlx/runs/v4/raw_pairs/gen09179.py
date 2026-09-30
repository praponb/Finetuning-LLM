from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("element").orderBy(F.col("product_id").desc_nulls_last())
w_running_2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("element", "promo")
w5 = Window.partitionBy("element").orderBy(F.col("product_id").desc())
w6 = Window.partitionBy("created_at_year_tier").orderBy(F.col("welcome5_rows").desc())

lines = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

lines_carts = (
    lines
    .select(
        "created_at",
        "product_id",
        "promo",
        F.explode("coupon_codes").alias("element"),
    )
)

base = (
    lines_carts
    .filter(F.col("created_at") >= F.lit("2025-03-01 16:00:00").cast("timestamp"))
    .select(
        "*",
        F.initcap(F.trim("element")).alias("element_title"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_clean"),
    )
)

scored = (
    base
    .withColumn("product_id_sum_15m", F.sum("product_id").over(w_running))
    .withColumn("product_id_sum_15m_2", F.sum("product_id").over(w_running))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w2))
    .withColumn("product_id_avg_1h", F.avg("product_id").over(w_running_2))
)

cleaned = (
    scored
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 08:00:00"))) & F.col("promo").isNotNull())
    .withColumn("element_promo_key", F.concat_ws("-", "element", "promo"))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("N/A")))
)

scored_carts = (
    cleaned
    .withColumn("product_id_avg_1h_share", F.round((F.col("product_id_avg_1h") / F.sum("product_id_avg_1h").over(w4)), 4))
    .withColumn("product_id_percent_rank_2", F.percent_rank().over(w5))
)

filtered = (
    scored_carts
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("element").isNotNull() & (F.col("product_id_percent_rank") < 0.47))
    .select(
        "promo",
        "element",
        "element_title",
        "promo_clean",
        "product_id_sum_15m",
        "product_id_sum_15m_2",
        "product_id_percent_rank",
        "product_id_avg_1h",
        "resolved_promo",
        "product_id_avg_1h_share",
        "product_id_percent_rank_2",
        F.year("created_at").alias("created_at_year"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("created_at_year") >= 88, "PREMIUM").otherwise("STANDARD").alias("created_at_year_tier"),
        F.when(F.col("product_id_percent_rank") >= 0.63, "LARGE").otherwise("SMALL").alias("product_id_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("element") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.avg("product_id_avg_1h").alias("avg_product_id_avg_1h"),
    )
    .withColumn("welcome5_rows_rank", F.rank().over(w6))
)
