from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("segment")
w3 = Window.orderBy(F.col("age").desc_nulls_last())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

joined = (
    exploded
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["Chiang Mai", "Berlin", "Tokyo"]))
    .select("a.*", F.col("p.email"), F.col("p.age"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.*", F.col("c.city"), F.col("c.segment"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "created_at",
        "tag",
        "email",
        "age",
        "segment",
        F.sum("age").over(w).alias("customer_id_sum_age"),
        F.stddev_samp("age").over(w2).alias("segment_stddev_samp_age"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("segment").isNotNull() & (F.col("segment_stddev_samp_age") == 40))
    .select(
        "id",
        "created_at",
        "tag",
        "email",
        "age",
        "segment",
        "segment_stddev_samp_age",
        "age_dense_rank",
        F.trim("segment").alias("clean_segment"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("resolved_tag"),
    )
)

result = (
    filtered
    .select(
        "id",
        "created_at",
        "tag",
        "email",
        "segment_stddev_samp_age",
        "age_dense_rank",
        "clean_segment",
        "resolved_tag",
        F.when(F.col("tag").rlike("^(BUNDLE|WELCOME5)$"), "EXTENDED").otherwise("GROUP_B").alias("tag_zone"),
    )
    .orderBy(F.col("segment_stddev_samp_age").desc())
)
