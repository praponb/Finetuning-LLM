from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_first())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("k", "customer_city").orderBy(F.col("age").desc_nulls_first())
w4 = Window.partitionBy("city")
w5 = Window.orderBy(F.col("age_share").desc_nulls_first())
w_part = Window.partitionBy("k")
w7 = Window.orderBy(F.col("city_stddev_samp_age").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

joined = (
    exploded
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.segment"), F.col("d.city"), F.col("d.name"))
)

joined_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.email"), F.col("b.age"))
)

metrics = (
    joined_carts
    .filter(F.col("city") == "Bangkok")
    .select(
        "customer_id",
        "k",
        "value",
        "city",
        "age",
        F.cume_dist().over(w).alias("age_cume_dist"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.*",
        F.col("c.signup_date"),
        F.col("c.city").alias("customer_city"),
        F.col("c.credit_limit"),
    )
)

scored = (
    with_dims
    .select(
        "customer_id",
        "k",
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
        F.max("credit_limit").over(w2).alias("customer_id_max_credit_limit"),
        F.stddev_samp("age").over(w4).alias("city_stddev_samp_age"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w5).alias("age_share_dense_rank"),
        F.avg("customer_id_max_credit_limit").over(w_part).alias("k_avg_customer_id_max_credit_limit"),
        F.round((F.col("age_dense_rank") / F.sum("age_dense_rank").over(w2)), 4).alias("age_dense_rank_share"),
        F.dense_rank().over(w7).alias("city_stddev_samp_age_dense_rank"),
        F.round((F.col("customer_id_max_credit_limit") / F.sum("customer_id_max_credit_limit").over(w2)), 4).alias("customer_id_max_credit_limit_share"),
    )
)

result = (
    with_windows
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.country").alias("country"),
        F.col("dim.segment").alias("segment"),
        F.col("e.k").alias("k"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.age_dense_rank_share")).alias("max_age_dense_rank_share"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("k")
)
