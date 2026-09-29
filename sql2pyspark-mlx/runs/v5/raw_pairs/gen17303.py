from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())
w2 = Window.orderBy(F.col("price").asc_nulls_last())
w_running = Window.orderBy(F.col("qty").desc_nulls_last())
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.orderBy(F.col("qty_rank").desc_nulls_first())

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.percent_rank().over(w).alias("price_percent_rank"),
        F.percent_rank().over(w2).alias("price_percent_rank_2"),
        F.rank().over(w_running).alias("qty_rank"),
    )
)

cleaned = (
    enriched
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & (F.col("product_id") > 4))
    .withColumn("price_percent_rank_minus_qty_rank", (F.col("price_percent_rank") - F.col("qty_rank")))
    .withColumn("qty_str", F.col("qty").cast("double"))
)

scored = (
    cleaned
    .filter(F.col("qty_str").between(21, 45))
    .select(
        "*",
        F.avg("price_percent_rank_2").over(w4).alias("price_percent_rank_2_avg_1h"),
        F.rank().over(w_ordered).alias("qty_rank_rank"),
    )
)

cleaned_carts = (
    scored
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "price",
        "price_percent_rank",
        "price_percent_rank_2",
        "qty_rank",
        "price_percent_rank_2_avg_1h",
        "qty_rank_rank",
        (F.col("price_percent_rank_2_avg_1h") * F.col("qty")).alias("price_percent_rank_2_avg_1h_times_qty"),
    )
)

valid = (
    cleaned_carts
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "*",
        F.round("qty_rank", 0).alias("qty_rank_rounded"),
        F.col("qty_rank_rank").cast("double").alias("qty_rank_rank_str"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("price_percent_rank_2_avg_1h_times_qty") >= 86, "HIGH").otherwise("LOW").alias("price_percent_rank_2_avg_1h_times_qty_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.max("created_at").alias("ended_at"),
        F.max("created_at").alias("last_seen"),
        F.count("*").alias("row_count"),
    )
)
