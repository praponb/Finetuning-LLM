from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("city_max_pos_rounded").desc_nulls_last())
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("price")
w6 = Window.partitionBy("country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.partitionBy("promo")
w_part = Window.partitionBy("customer_id").orderBy(F.col("product_id_count_1h").desc_nulls_last())
w9 = Window.partitionBy("promo").orderBy(F.col("pos").desc_nulls_first())
w_ordered = Window.partitionBy("country").orderBy(F.col("pos").desc_nulls_first())
w11 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.city"), F.col("p.email"))
)

scored = (
    with_dims
    .select(
        "*",
        F.max("pos").over(w).alias("city_max_pos"),
        F.sum("pos").over(w2).alias("pos_sum_30m"),
        F.sum("qty").over(w2).alias("qty_sum_30m"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.name"), F.col("p.age"), F.col("p.country"))
)

valid = (
    joined
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        "product_id",
        "qty",
        "price",
        "promo",
        "city",
        "email",
        "city_max_pos",
        "pos_sum_30m",
        "qty_sum_30m",
        "name",
        "country",
        (F.col("qty_sum_30m") - F.col("product_id")).alias("qty_sum_30m_minus_product_id"),
        F.round("city_max_pos", 1).alias("city_max_pos_rounded"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

scored_carts = (
    valid
    .select(
        "*",
        F.dense_rank().over(w3).alias("city_max_pos_rounded_dense_rank"),
        F.sum("qty_sum_30m").over(w4).alias("qty_sum_30m_sum_30m"),
        F.dense_rank().over(w5).alias("price_dense_rank"),
        F.count("product_id").over(w6).alias("product_id_count_1h"),
        F.round((F.col("qty") / F.sum("qty").over(w7)), 4).alias("qty_share"),
    )
)

scored_carts_2 = (
    scored_carts
    .withColumn("product_id_count_1h_cume_dist", F.cume_dist().over(w_part))
    .withColumn("pos_percent_rank", F.percent_rank().over(w9))
    .withColumn("pos_rank", F.rank().over(w_ordered))
    .withColumn("city_max_pos_rounded_avg_30m", F.avg("city_max_pos_rounded").over(w11))
    .withColumn("qty_sum_30m_share", F.round((F.col("qty_sum_30m") / F.sum("qty_sum_30m").over(w7)), 4))
)

result = (
    scored_carts_2
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        "promo",
        "city_max_pos",
        "pos_sum_30m",
        "qty_sum_30m",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
