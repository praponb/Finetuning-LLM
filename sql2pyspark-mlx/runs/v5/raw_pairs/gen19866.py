from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id")
w_recent_2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc_nulls_last())

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

with_dims = (
    exploded
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.value",
        F.col("b.credit_limit"),
        F.col("b.segment"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("customer_id").isNotNull() & F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .select(
        "id",
        "customer_id",
        "credit_limit",
        "segment",
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "Group A").otherwise("Other").alias("segment_group"),
        F.concat_ws("/", "value", "segment").alias("value_segment_key"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "credit_limit",
        "value_segment_key",
        F.stddev_samp("credit_limit").over(w_recent).alias("customer_id_stddev_samp_credit_limit"),
        F.cume_dist().over(w_recent_2).alias("credit_limit_cume_dist"),
        F.avg("credit_limit").over(w_recent).alias("customer_id_avg_credit_limit"),
        F.rank().over(w3).alias("credit_limit_rank"),
        F.min("credit_limit").over(w_recent).alias("customer_id_min_credit_limit"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("credit_limit") >= 14800, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("customer_id_stddev_samp_credit_limit", 0.5).alias("customer_id_stddev_samp_credit_limit_p50"),
        F.round(F.avg("customer_id_min_credit_limit"), 2).alias("avg_customer_id_min_credit_limit"),
        F.avg("customer_id_min_credit_limit").alias("avg_customer_id_min_credit_limit_2"),
        F.avg("credit_limit_rank").alias("avg_credit_limit_rank"),
    )
    .orderBy(F.col("credit_limit_tier").asc_nulls_last())
)
