from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

base = (
    exploded
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .withColumn("created_at_period", F.date_trunc("MONTH", "created_at"))
)

joined = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.city") != "London")
    .select(
        "e.id",
        "e.customer_id",
        "e.coupon_codes",
        "e.key",
        "e.v",
        "e.created_at_period",
        F.col("b.signup_date"),
        F.col("b.city"),
        F.col("b.country"),
    )
)

joined_carts = (
    joined
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.name"), F.col("d.city").alias("customer_city"))
)

result = (
    joined_carts
    .rollup(
        F.date_trunc("YEAR", "created_at_period").alias("year_start"),
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count(F.when(F.col("customer_city") == "Berlin", 1)).alias("berlin_rows"),
        ((F.unix_timestamp(F.max("created_at_period")) - F.unix_timestamp(F.min("created_at_period"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("customer_city") == "Berlin", 1)).alias("berlin_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .orderBy(F.col("jp_count").desc_nulls_last())
)
