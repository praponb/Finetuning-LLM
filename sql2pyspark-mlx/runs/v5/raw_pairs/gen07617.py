from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy("country_length", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy("started_at", "id")

cleaned = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "credit_limit",
        "age",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.length("country").alias("country_length"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped
    .filter(F.col("name").like("M%"))
    .select(
        "*",
        F.coalesce("age", F.lit(0)).alias("age_filled"),
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"))
)

latest = (
    with_latest
    .withColumn("rank_in_group_2", F.row_number().over(w3))
    .filter(F.col("rank_in_group_2") == 1)
    .select(
        "id",
        "name",
        "city",
        "credit_limit",
        "age",
        "city_prefix",
        "country_length",
        "resolved_city",
        "started_at",
    )
)

matched_keys = (
    latest
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("id", "city")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.plan") == "premium"), "left_anti")
)

result = (
    eligible
    .withColumn("country_length_tier", F.when(F.col("country_length") >= 79, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .groupingSets(
        [["country_length_tier", "year_start"], ["country_length_tier"], []],
        "country_length_tier",
        "year_start",
    )
    .agg(
        F.avg(F.when(F.col("city") == "Chiang Mai", F.col("age"))).alias("chiang_mai_avg_age"),
        F.sum("country_length").alias("total_country_length"),
        F.max("started_at").alias("last_started_at"),
        F.max(F.when(F.col("city") == "Chiang Mai", F.col("credit_limit"))).alias("chiang_mai_max_credit_limit"),
        F.min("country_length").alias("min_country_length"),
        F.percentile_approx("country_length", [0.25, 0.5, 0.75]).alias("country_length_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("chiang_mai_avg_age")
)
