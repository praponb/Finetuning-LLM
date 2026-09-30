from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").desc_nulls_last())
w2 = Window.orderBy(F.col("product_id").desc())
w3 = Window.partitionBy("tag").orderBy("product_id_percent_rank")
w4 = Window.orderBy(F.col("product_id_cume_dist").desc())

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = exploded.select("product_id", F.explode("coupon_codes").alias("tag"))

enriched = (
    flattened
    .withColumn("product_id_percent_rank", F.percent_rank().over(w))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w2))
)

enriched_carts = (
    enriched
    .withColumn("product_id_percent_rank_rank", F.rank().over(w3))
    .withColumn("product_id_cume_dist_rank", F.rank().over(w4))
)

base = (
    enriched_carts
    .filter(F.col("product_id_cume_dist") <= 0.38)
    .withColumn("product_id_percent_rank_rank_times_product_id", (F.col("product_id_percent_rank_rank") * F.col("product_id")))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("NONE")))
)

result = (
    base
    .withColumn("product_id_cume_dist_tier", F.when(F.col("product_id_cume_dist") >= 0.78, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["tag", "product_id_cume_dist_tier"], ["tag"]],
        "tag",
        "product_id_cume_dist_tier",
    )
    .agg(
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.sort_array(F.collect_set("tag")).alias("tag_list_2"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("cnt") > 1)
)
