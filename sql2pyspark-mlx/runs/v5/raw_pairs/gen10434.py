from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("product_id").desc_nulls_last())
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.orderBy(F.col("price_max_15m").desc())
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("avg_product_id").desc())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

enriched = (
    exploded
    .filter(F.col("promo") == "WELCOME5")
    .select(
        "id",
        "created_at",
        "product_id",
        F.max("price").over(w).alias("price_max_15m"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
    )
)

metrics = (
    enriched
    .select(
        "created_at",
        "product_id",
        "price_max_15m",
        "product_id_share",
        F.dense_rank().over(w3).alias("product_id_dense_rank"),
        F.avg("price_max_15m").over(w4).alias("price_max_15m_avg_30m"),
    )
)

valid = (
    metrics
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("product_id") < 8))
    .withColumn("product_id_share_whole", F.ceil("product_id_share"))
    .withColumn("product_id_times_price_max_15m", (F.col("product_id") * F.col("price_max_15m")))
)

enriched_carts = (
    valid
    .select(
        "created_at",
        "product_id",
        "price_max_15m",
        "price_max_15m_avg_30m",
        "product_id_times_price_max_15m",
        F.percent_rank().over(w5).alias("price_max_15m_percent_rank"),
        F.sum("product_id").over(w6).alias("product_id_sum_2h"),
    )
)

result = (
    enriched_carts
    .groupBy(
        "product_id",
        F.when(F.col("price_max_15m_percent_rank") >= 0.71, "HIGH").otherwise("LOW").alias("price_max_15m_percent_rank_tier"),
        F.when(F.col("price_max_15m") >= 187, "PREMIUM").otherwise("STANDARD").alias("price_max_15m_tier"),
    )
    .agg(
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.count("*").alias("row_count"),
    )
    .withColumn("avg_product_id_rank", F.rank().over(w7))
)
