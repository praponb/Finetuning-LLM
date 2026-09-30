from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("adjusted_quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w_running = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("latest_rank").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("store_id_min_quantity").desc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-13").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 0.85), 2).alias("adjusted_quantity"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    latest
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.last_restock",
        "s.latest_rank",
        F.col("b.opened_date"),
    )
)

enriched = (
    joined
    .filter(F.col("latest_rank") != 1)
    .select(
        "*",
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
        F.count("quantity").over(w_running).alias("store_id_count_quantity"),
        F.row_number().over(w4).alias("seq_no"),
        F.min("quantity").over(w_running).alias("store_id_min_quantity"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.*",
        F.col("s.opened_date").alias("store_opened_date"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank_2", F.row_number().over(w5))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("quantity_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_dense_rank_tier"),
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
        F.when(F.col("store_id_count_quantity") >= 13, "HIGH").otherwise("LOW").alias("store_id_count_quantity_tier"),
    )
    .agg(
        F.max(F.when(F.col("city") == "Bangkok", F.col("quantity_dense_rank"))).alias("bangkok_max_quantity_dense_rank"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("store_id_count_quantity")).otherwise(0)).alias("chiang_mai_store_id_count_quantity"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .orderBy(F.col("quantity_p50").desc_nulls_last())
)
