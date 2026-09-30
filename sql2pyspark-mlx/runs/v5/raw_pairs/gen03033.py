from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("uk_rows").desc())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

exploded = lines.select("customer_id", "key", F.explode_outer("coupon_codes").alias("value_2"))

enriched = (
    exploded
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.country"))
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "src.customer_id",
        "src.country",
        F.col("d.age"),
        F.col("d.email"),
        F.col("d.segment"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select(
        "e.customer_id",
        "e.country",
        "e.age",
        "e.segment",
        F.col("d.segment").alias("customer_segment"),
        F.col("d.country").alias("customer_country"),
        F.col("d.name"),
    )
)

valid = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .withColumn("country_name", F.concat("country", F.lit(", "), "name"))
    .withColumn("customer_country_segment_key", F.concat_ws(",", "customer_country", "segment"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("OTHER")))
)

with_dims_carts = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.customer_id",
        "s.country",
        "s.age",
        "s.segment",
        "s.customer_segment",
        "s.customer_country",
        "s.name",
        "s.country_name",
        "s.resolved_country",
        F.col("b.signup_date"),
    )
)

totals = (
    with_dims_carts
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.size(F.collect_set("customer_country")).alias("num_customer_countries"),
        F.count(F.when(F.col("customer_country") == "UK", 1)).alias("uk_rows"),
        F.sum(F.when(F.col("country") == "JP", F.col("age"))).alias("jp_total_age"),
    )
    .withColumn("uk_rows_rank", F.rank().over(w))
)

result = (
    totals
    .select("week_start", "activity_month", "num_customer_countries", "jp_total_age")
    .orderBy(F.col("week_start").asc_nulls_first(), "activity_month")
)
