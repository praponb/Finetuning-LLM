from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code", "promo").orderBy(F.col("product_id").asc_nulls_first())
w2 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty").asc_nulls_first())
w4 = Window.partitionBy("promo").orderBy(F.col("product_id").desc())
w5 = Window.partitionBy("segment")
w_recent = Window.orderBy(F.col("product_id_cume_dist").asc_nulls_last())

flattened = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "product_id",
        "qty",
        "promo",
        F.explode("coupon_codes").alias("code"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.dense_rank().over(w2).alias("product_id_dense_rank"),
        F.rank().over(w3).alias("qty_rank"),
        F.dense_rank().over(w4).alias("product_id_dense_rank_2"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.city"), F.col("p.name"), F.col("p.segment"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.country"),
        F.col("b.segment").alias("customer_segment"),
        F.col("b.credit_limit"),
    )
)

enriched_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.*",
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("c.country").alias("customer_country"),
    )
)

result = (
    enriched_carts
    .withColumn("qty_rank_share", F.round((F.col("qty_rank") / F.sum("qty_rank").over(w5)), 4))
    .withColumn("product_id_cume_dist_dense_rank", F.dense_rank().over(w_recent))
)
