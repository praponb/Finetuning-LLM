from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("pos").asc_nulls_last(), "id", "pos")
w2 = Window.partitionBy("promo")
w_running = Window.orderBy("product_id")
w4 = Window.partitionBy("promo").orderBy(F.col("product_id").asc_nulls_first())
w5 = Window.orderBy("product_id_rank")
w6 = Window.orderBy(F.col("product_id").asc_nulls_last())

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select("id", "coupon_codes", "pos", F.col("item.product_id"), F.col("item.promo"))
)

latest = (
    lines
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("product_id") >= 4)
    .select(
        "id",
        "coupon_codes",
        "pos",
        "product_id",
        "promo",
        "row_num",
        (F.col("pos") + F.col("row_num")).alias("pos_plus_row_num"),
        F.coalesce("promo", F.lit("OTHER")).alias("promo_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("row_num") != 1)
    .withColumn("pos_minus_product_id", (F.col("pos") - F.col("product_id")))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("OTHER")))
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("promo_sum_pos", F.sum("pos").over(w2))
    .withColumn("product_id_rank", F.rank().over(w_running))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w4))
)

scored = (
    enriched
    .select(
        "*",
        F.avg("pos").over(w2).alias("promo_avg_pos"),
        F.rank().over(w5).alias("product_id_rank_rank"),
        F.cume_dist().over(w6).alias("product_id_cume_dist_2"),
    )
)

result = (
    scored
    .withColumn("promo_avg_pos_tier", F.when(F.col("promo_avg_pos") >= 4, "HIGH").otherwise("LOW"))
    .withColumn("product_id_cume_dist_2_tier", F.when(F.col("product_id_cume_dist_2") >= 0.63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["promo", "promo_avg_pos_tier", "product_id_cume_dist_2_tier"], ["promo", "promo_avg_pos_tier"]],
        "promo",
        "promo_avg_pos_tier",
        "product_id_cume_dist_2_tier",
    )
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.grouping("promo").alias("gid"),
    )
)
