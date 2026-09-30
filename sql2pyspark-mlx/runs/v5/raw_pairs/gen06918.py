from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("customer_id", "city")
w4 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_last())
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("promo", "customer_id").orderBy("qty")

exploded = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

enriched = (
    exploded
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.segment").isNotNull())
    .select("e.*", F.col("p.credit_limit"), F.col("p.city"))
)

enriched_carts = (
    enriched
    .withColumn("promo_min_credit_limit", F.min("credit_limit").over(w))
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w2))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w3)), 4))
    .withColumn("price_share_2", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("promo_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w))
)

scored = (
    enriched_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        "city",
        "promo_min_credit_limit",
        "credit_limit_percent_rank",
        "price_share_2",
        "promo_stddev_samp_credit_limit",
        F.percent_rank().over(w4).alias("qty_percent_rank"),
        F.max("price_share").over(w5).alias("city_max_price_share"),
        F.dense_rank().over(w6).alias("qty_dense_rank"),
    )
)

daily = (
    scored
    .withColumn("qty_tier", F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW"))
    .cube("qty_tier")
    .agg(
        F.sum(F.when(F.col("promo") == "WELCOME5", F.col("qty_percent_rank")).otherwise(0)).alias("welcome5_qty_percent_rank"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.round(F.avg("promo_min_credit_limit"), 2).alias("avg_promo_min_credit_limit"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("welcome5_qty_percent_rank") >= 0)
)

result = (
    daily
    .select("qty_tier", "welcome5_qty_percent_rank", "avg_qty", "gid")
    .orderBy(F.col("gid").desc_nulls_first())
)
