from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_part = Window.partitionBy("promo").orderBy(F.col("product_id").asc_nulls_first())
w3 = Window.partitionBy("promo")
w4 = Window.partitionBy("customer_id").orderBy(F.col("product_id").asc_nulls_last())
w5 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())
w6 = Window.partitionBy("customer_id")
w7 = Window.orderBy(F.col("product_id_dense_rank").desc_nulls_first())
w8 = Window.partitionBy("promo").orderBy(F.col("customer_id_max_product_id").desc())

exploded = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

remaining_keys = (
    exploded
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.col("customer_id") == 23)
        .select("customer_id", "id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("price_sum_1h", F.sum("price").over(w))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w_part))
)

valid = (
    metrics
    .filter(F.col("created_at") > F.lit("2025-03-02 03:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "product_id",
        "promo",
        F.lower("promo").alias("promo_lower"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("resolved_promo"),
    )
)

metrics_carts = (
    valid
    .withColumn("promo_sum_product_id", F.sum("product_id").over(w3))
    .withColumn("product_id_dense_rank", F.dense_rank().over(w4))
    .withColumn("product_id_dense_rank_2", F.dense_rank().over(w5))
    .withColumn("customer_id_max_product_id", F.max("product_id").over(w6))
)

enriched = (
    metrics_carts
    .select(
        "*",
        F.round((F.col("customer_id_max_product_id") / F.sum("customer_id_max_product_id").over(w6)), 4).alias("customer_id_max_product_id_share"),
        F.rank().over(w7).alias("product_id_dense_rank_rank"),
        F.cume_dist().over(w8).alias("customer_id_max_product_id_cume_dist"),
    )
)

filtered = (
    enriched
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 22:00:00"))) & F.col("promo").isNotNull())
    .select(
        "customer_id",
        "promo",
        "promo_lower",
        "promo_sum_product_id",
        "product_id_dense_rank",
        "product_id_dense_rank_2",
        "customer_id_max_product_id",
        "customer_id_max_product_id_share",
        F.date_trunc("MONTH", "created_at").alias("created_at_period"),
    )
)

result = (
    filtered
    .select(
        "product_id_dense_rank",
        "product_id_dense_rank_2",
        "customer_id_max_product_id",
        F.avg("customer_id_max_product_id_share").over(w6).alias("customer_id_avg_customer_id_max_product_id_share"),
    )
)
