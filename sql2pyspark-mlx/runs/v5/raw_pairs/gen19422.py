from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment")
w_recent = Window.partitionBy("segment").orderBy("signup_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & (F.col("country") != "TH"))
    .withColumn("city_prefix", F.substring("city", 1, 3))
)

filtered_customers = (
    filtered
    .filter((~F.col("city").eqNullSafe(F.lit("Chiang Mai"))) & F.col("city").isNotNull())
    .select(
        "*",
        F.concat("country", F.lit("/"), "email").alias("country_email"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

enriched = (
    filtered_customers
    .select(
        "*",
        F.corr("age", "credit_limit").over(w_grp).alias("corr_age_credit_limit"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.min("age").over(w_grp).alias("segment_min_age"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser"))
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.discount"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    with_last_customers
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .rollup("country", "activity_month", "segment")
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.sum(F.when(F.col("browser") == "Firefox", F.col("age"))).alias("firefox_total_age"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("event_count").desc())
)
