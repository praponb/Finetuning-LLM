from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id")
w4 = Window.orderBy(F.col("quantity").desc())
w5 = Window.partitionBy("product_id", "order_id")
w6 = Window.orderBy(F.col("quantity_rank_share").asc_nulls_first())
w7 = Window.partitionBy("quantity_tier").orderBy(F.col("distinct_ids_2").desc())

remaining_keys = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") < 3)
        .select("id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .filter(F.col("order_id").isin([15, 23, 35, 40]))
    .select(
        "*",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.count("unit_price").over(w2).alias("product_id_count_unit_price"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.rank().over(w4).alias("quantity_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("product_id_count_unit_price").between(7, 9))
    .withColumn("quantity_rank_filled", F.coalesce("quantity_rank", F.lit(0)))
    .withColumn("adjusted_unit_price_share", F.round((F.col("unit_price_share") * 1.15), 2))
)

cleaned = (
    filtered
    .filter(F.col("order_id") == 7)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "quantity_dense_rank",
        "product_id_count_unit_price",
        "unit_price_share",
        "quantity_rank",
        "quantity_rank_filled",
        F.coalesce("quantity_rank_filled", F.lit(0)).alias("quantity_rank_filled_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "quantity",
        "quantity_rank_filled",
        "quantity_rank_filled_filled",
        F.round((F.col("quantity_rank") / F.sum("quantity_rank").over(w5)), 4).alias("quantity_rank_share"),
        F.count("quantity_rank_filled_filled").over(w2).alias("product_id_count_quantity_rank_filled_filled"),
        F.min("quantity_rank_filled_filled").over(w3).alias("order_id_min_quantity_rank_filled_filled"),
    )
)

enriched = (
    metrics
    .select("*", F.percent_rank().over(w6).alias("quantity_rank_share_percent_rank"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        F.when(F.col("quantity_rank_share") >= 0.65, "HIGH").otherwise("LOW").alias("quantity_rank_share_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .withColumn("distinct_ids_2_rank", F.rank().over(w7))
)
