from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc_nulls_first())
w_running = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("product_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("price_scaled").desc_nulls_first())
w5 = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_first())
w6 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.price"),
    )
)

valid = (
    flattened
    .filter(F.col("customer_id").isNotNull())
    .withColumn("price_scaled", F.round((F.col("price") * 0.9), 2))
)

enriched = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("price_rank"),
        F.sum("price_scaled").over(w_running).alias("customer_id_sum_price_scaled"),
        F.round((F.col("product_id") / F.sum("product_id").over(w_running)), 4).alias("product_id_share"),
        F.cume_dist().over(w3).alias("product_id_cume_dist"),
        F.dense_rank().over(w4).alias("price_scaled_dense_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.rank().over(w5).alias("product_id_rank"),
        F.avg("price").over(w_running).alias("customer_id_avg_price"),
        F.sum("price_scaled").over(w6).alias("price_scaled_sum_1h"),
    )
)

filtered = (
    with_windows
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & (F.col("price_rank") >= 7))
    .select(
        "*",
        F.col("customer_id_avg_price").cast("double").alias("customer_id_avg_price_str"),
    )
)

cleaned = (
    filtered
    .filter(F.to_date("created_at") < "2024-01-16")
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "price",
        "price_scaled",
        "price_rank",
        "customer_id_sum_price_scaled",
        "product_id_cume_dist",
        "product_id_rank",
        "customer_id_avg_price",
        "price_scaled_sum_1h",
        "customer_id_avg_price_str",
        F.greatest("price_scaled_dense_rank", "price_scaled").alias("greatest_price_scaled_dense_rank_price_scaled"),
    )
)

result = (
    cleaned
    .select(
        "customer_id",
        "created_at",
        "price",
        "price_scaled",
        "price_rank",
        "product_id_cume_dist",
        "product_id_rank",
        "price_scaled_sum_1h",
        "customer_id_avg_price_str",
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
