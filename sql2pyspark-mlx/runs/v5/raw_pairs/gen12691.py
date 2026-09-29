from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").asc_nulls_last())
w_recent = Window.partitionBy("element").orderBy(F.col("product_id").desc())
w3 = Window.partitionBy("element").orderBy(F.col("product_id").desc_nulls_first())
w4 = Window.orderBy(F.col("product_id_cume_dist_2").asc_nulls_last())

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

exploded = flattened.select("product_id", F.explode_outer("coupon_codes").alias("element"))

enriched = (
    exploded
    .filter(F.col("element") == "SPRING10")
    .select(
        "product_id",
        "element",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.cume_dist().over(w_recent).alias("product_id_cume_dist_2"),
        F.dense_rank().over(w3).alias("product_id_dense_rank"),
    )
)

scored = (
    enriched
    .select(
        "product_id_cume_dist",
        "product_id_cume_dist_2",
        "product_id_dense_rank",
        F.rank().over(w4).alias("product_id_cume_dist_2_rank"),
    )
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("product_id_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("product_id_dense_rank_tier"),
        F.when(F.col("product_id_cume_dist_2_rank") >= 7, "LARGE").otherwise("SMALL").alias("product_id_cume_dist_2_rank_tier"),
    )
    .agg(
        F.round(F.avg("product_id_dense_rank"), 2).alias("avg_product_id_dense_rank"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
    )
)

result = (
    grouped
    .select(
        "product_id_dense_rank_tier",
        "product_id_cume_dist_2_rank_tier",
        "avg_product_id_dense_rank",
        "event_count",
        "event_count_2",
    )
    .orderBy(F.col("product_id_cume_dist_2_rank_tier").desc(), "event_count_2")
)
