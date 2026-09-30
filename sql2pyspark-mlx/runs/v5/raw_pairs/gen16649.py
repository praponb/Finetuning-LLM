from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("code").orderBy(F.col("credit_limit_filled").asc_nulls_last())
w3 = Window.partitionBy("code", "attr_key")
w4 = Window.orderBy(F.col("credit_limit_filled").asc_nulls_first())
w5 = Window.partitionBy("attr_key", "code")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "attr_key",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

joined = (
    exploded
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.email"), F.col("b.credit_limit"))
)

cleaned = (
    joined
    .filter(F.col("email").isNull())
    .select(
        "attr_key",
        "code",
        "email",
        "credit_limit",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
        F.upper("email").alias("email_upper"),
    )
)

scored = (
    cleaned
    .filter(F.col("code") == "BUNDLE")
    .select(
        "*",
        F.stddev_samp("credit_limit").over(w).alias("attr_key_stddev_samp_credit_limit"),
        F.rank().over(w2).alias("credit_limit_filled_rank"),
        F.sum("credit_limit_filled").over(w3).alias("code_sum_credit_limit_filled"),
    )
)

metrics = (
    scored
    .select(
        "attr_key",
        "code",
        "email",
        "credit_limit_filled",
        "credit_limit_filled_rank",
        "code_sum_credit_limit_filled",
        F.dense_rank().over(w4).alias("credit_limit_filled_dense_rank"),
        F.count("credit_limit_filled").over(w3).alias("code_count_credit_limit_filled"),
        F.stddev_samp("credit_limit_filled_rank").over(w5).alias("attr_key_stddev_samp_credit_limit_filled_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("code_count_credit_limit_filled") != 6)
    .withColumn("credit_limit_filled_plus_attr_key_stddev_samp_credit_limit_filled_rank", (F.col("credit_limit_filled") + F.col("attr_key_stddev_samp_credit_limit_filled_rank")))
)

stats = (
    valid
    .groupBy(
        "code",
        F.when(F.col("credit_limit_filled_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("credit_limit_filled_dense_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.min("credit_limit_filled_rank").alias("min_credit_limit_filled_rank"),
        F.sum(F.when(F.col("code") == "BUNDLE", F.col("credit_limit_filled")).otherwise(0)).alias("bundle_credit_limit_filled"),
        F.percentile_approx("credit_limit_filled", 0.95).alias("credit_limit_filled_p95"),
        F.sum(F.when(F.col("code") == "WELCOME5", F.col("code_sum_credit_limit_filled")).otherwise(0)).alias("welcome5_code_sum_credit_limit_filled"),
    )
    .filter(F.col("credit_limit_filled_p95") >= 0)
)

result = (
    stats
    .select(
        "code",
        "credit_limit_filled_dense_rank_tier",
        "attr_key_list",
        "credit_limit_filled_p95",
    )
)
