from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("price").asc_nulls_last())
w3 = Window.partitionBy("customer_id", "promo")

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

scored = (
    exploded
    .select(
        "coupon_codes",
        "product_id",
        "qty",
        F.count("price").over(w).alias("price_count_1h"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.stddev_samp("product_id").over(w3).alias("customer_id_stddev_samp_product_id"),
    )
)

valid = (
    scored
    .filter(F.col("customer_id_stddev_samp_product_id") == 4)
    .select(
        "coupon_codes",
        "qty",
        "price_dense_rank",
        "customer_id_stddev_samp_product_id",
        F.round((F.col("qty") / F.col("price_count_1h")), 2).alias("qty_per_price_count_1h"),
    )
)

valid_carts = (
    valid
    .filter(F.col("customer_id_stddev_samp_product_id") > 6)
    .withColumn("qty_num", F.col("qty").cast("string"))
)

result = (
    valid_carts
    .groupBy("customer_id_stddev_samp_product_id")
    .agg(
        F.sum("customer_id_stddev_samp_product_id").alias("total_customer_id_stddev_samp_product_id"),
        F.percentile_approx("price_dense_rank", [0.25, 0.5, 0.75]).alias("price_dense_rank_quartiles"),
    )
)
