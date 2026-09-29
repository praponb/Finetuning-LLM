from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.orderBy(F.col("qty").desc_nulls_last())
w3 = Window.partitionBy("tag").orderBy(F.col("product_id").desc_nulls_first())
w_running = Window.partitionBy("tag", "promo").orderBy(F.col("qty_percent_rank").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "id",
        "product_id",
        "qty",
        "promo",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

with_windows = (
    exploded_carts
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w)), 4))
    .withColumn("qty_percent_rank", F.percent_rank().over(w2))
    .withColumn("product_id_rank", F.rank().over(w3))
)

metrics = (
    with_windows
    .select(
        "product_id",
        "qty",
        "promo",
        "qty_share",
        "qty_percent_rank",
        "product_id_rank",
        F.percent_rank().over(w_running).alias("qty_percent_rank_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("product_id_rank") >= 7, "HIGH").otherwise("LOW").alias("product_id_rank_tier"),
        "product_id",
    )
    .agg(
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .orderBy(F.col("product_id").asc_nulls_last())
)
