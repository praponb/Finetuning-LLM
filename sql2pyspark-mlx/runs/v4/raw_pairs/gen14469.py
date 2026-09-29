from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_last())
w3 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_last())

flattened = df_carts.select("id", F.inline("items"))

metrics = (
    flattened
    .select(
        "*",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.dense_rank().over(w2).alias("qty_dense_rank"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("price_percent_rank") <= 0.49)
    .withColumn("promo_length", F.length("promo"))
)

summary = (
    valid
    .rollup(
        "product_id",
        F.when(F.col("price_percent_rank") >= 0.82, "LARGE").otherwise("SMALL").alias("price_percent_rank_tier"),
    )
    .agg(
        F.avg(F.when(F.col("promo") == "SPRING10", F.col("price_percent_rank"))).alias("spring10_avg_price_percent_rank"),
        F.percentile_approx("price_percent_rank", 0.5).alias("price_percent_rank_p50"),
        F.min("qty_dense_rank").alias("min_qty_dense_rank"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("qty_percent_rank")).otherwise(0)).alias("vip_qty_percent_rank"),
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("price_percent_rank")).otherwise(0)).alias("welcome5_price_percent_rank"),
    )
)

result = (
    summary
    .select(
        "product_id",
        "price_percent_rank_tier",
        "spring10_avg_price_percent_rank",
        "price_percent_rank_p50",
        "welcome5_rows",
        "vip_qty_percent_rank",
    )
    .orderBy(F.col("welcome5_rows").desc_nulls_last(), "price_percent_rank_p50")
)
