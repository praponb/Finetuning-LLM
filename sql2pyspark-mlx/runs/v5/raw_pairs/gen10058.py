from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "code").orderBy(F.col("is_bundle").desc_nulls_last())
w2 = Window.partitionBy("customer_id")
w_grp = Window.partitionBy("code", "customer_id").orderBy(F.col("is_bundle").asc_nulls_first())
w4 = Window.partitionBy("code")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

base = (
    exploded
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & F.col("code").isNotNull())
    .select("*", F.date_format("created_at", "yyyy-MM").alias("created_at_label"))
)

enriched = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.email"), F.col("b.name"), F.col("b.country"))
)

cleaned = (
    enriched
    .filter(~F.col("code").eqNullSafe(F.lit("WELCOME5")))
    .withColumn("is_bundle", F.when(F.col("code") == "BUNDLE", 1).otherwise(0))
    .withColumn("name_length", F.length("name"))
)

valid = (
    cleaned
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (~F.col("code").eqNullSafe(F.lit("WELCOME5"))))
    .select("*", F.lower("name").alias("name_lower"))
)

metrics = (
    valid
    .filter(F.col("name").like("%Ross%"))
    .withColumn("is_bundle_cume_dist", F.cume_dist().over(w))
    .withColumn("customer_id_stddev_samp_is_bundle", F.stddev_samp("is_bundle").over(w2))
    .withColumn("is_bundle_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("code_avg_is_bundle", F.avg("is_bundle").over(w4))
)

result = (
    metrics
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.round(F.sum("is_bundle_cume_dist") / F.nullif(F.count(F.when(F.col("code") == "BUNDLE", 1)), F.lit(0)), 2).alias("is_bundle_cume_dist_per_bundle"),
        F.min("created_at").alias("first_created_at"),
        F.round(F.sum("is_bundle_dense_rank") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("is_bundle_dense_rank_per_sg"),
    )
    .orderBy(F.col("is_bundle_dense_rank_per_sg").desc_nulls_last())
)
