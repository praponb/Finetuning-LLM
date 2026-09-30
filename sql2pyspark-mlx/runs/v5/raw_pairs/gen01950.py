from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "k").orderBy("age", "id", "k")
w2 = Window.partitionBy("k").orderBy("age", "id", "k").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("age").desc())
w_grp = Window.orderBy(F.col("age").asc_nulls_last())
w5 = Window.partitionBy("k", "segment").orderBy("next_age", "id", "k")
w6 = Window.partitionBy("k").orderBy("least_age_age_cume_dist", "id", "k")
w7 = Window.orderBy("customer_age", "id", "k").rowsBetween(-2, -1)
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("age_cume_dist").desc_nulls_last())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

enriched = (
    lines
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.k",
        F.col("b.age"),
        F.col("b.segment"),
        F.col("b.country"),
    )
)

metrics = (
    enriched
    .filter(F.col("country") == "DE")
    .withColumn("next_age", F.lead("age", 1).over(w))
    .withColumn("age_avg_6", F.avg("age").over(w2))
    .withColumn("age_cume_dist", F.cume_dist().over(w3))
)

with_dims = (
    metrics
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.k",
        "s.age",
        "s.segment",
        "s.country",
        "s.next_age",
        "s.age_cume_dist",
        F.col("b.age").alias("customer_age"),
        F.col("b.name"),
    )
)

base = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .withColumn("age_cume_dist_rounded", F.round("age_cume_dist", 2))
    .withColumn("least_age_age_cume_dist", F.least("age", "age_cume_dist"))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

enriched_carts = (
    base
    .select(
        "*",
        F.rank().over(w_grp).alias("age_rank"),
        F.ntile(5).over(w5).alias("next_age_bucket"),
        F.ntile(3).over(w6).alias("least_age_age_cume_dist_bucket"),
        F.avg("next_age").over(w7).alias("next_age_avg_2_prior"),
        F.dense_rank().over(w_ordered).alias("age_cume_dist_dense_rank"),
    )
)

result = (
    enriched_carts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.country").alias("customer_country"),
        F.col("s.segment").alias("cart_segment"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.next_age_avg_2_prior")).alias("min_next_age_avg_2_prior"),
        F.min(F.col("s.age_cume_dist_rounded")).alias("min_age_cume_dist_rounded"),
    )
)
