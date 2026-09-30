from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("price").desc())
w3 = Window.partitionBy("promo").orderBy(F.col("price").asc_nulls_last())

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

scored = (
    lines
    .select(
        "*",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.cume_dist().over(w3).alias("price_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("promo").isNotNull())
    .select(
        "*",
        (F.col("price") * F.col("price_dense_rank")).alias("price_times_price_dense_rank"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("qty") >= 4, "LARGE").otherwise("SMALL").alias("qty_tier"),
        F.when(F.col("price_dense_rank") >= 6, "LARGE").otherwise("SMALL").alias("price_dense_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("qty_share"))).alias("welcome5_total_qty_share"),
        F.countDistinct("promo").alias("distinct_promos"),
    )
    .orderBy(F.col("welcome5_total_qty_share").desc_nulls_last())
)
