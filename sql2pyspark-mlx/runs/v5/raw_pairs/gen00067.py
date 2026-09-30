from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age_plus_credit_limit", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("segment")
w3 = Window.orderBy("age_plus_credit_limit", "id")
w4 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w_part = Window.partitionBy("segment").orderBy("age", "id")
w6 = Window.partitionBy("segment").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-10").cast("date")) & F.col("email").like("%@startup.io"))
    .select(
        "id",
        "name",
        "segment",
        "is_active",
        "age",
        F.regexp_replace("name", " ", "_").alias("name_slug"),
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
    )
)

metrics = (
    filtered
    .filter(F.col("age") >= 51)
    .select(
        "id",
        "segment",
        "age",
        "name_slug",
        F.avg("age").over(w).alias("age_avg_5"),
        F.count("age_plus_credit_limit").over(w2).alias("segment_count_age_plus_credit_limit"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .withColumn("prev_age_avg_5", F.lag("age_avg_5").over(w_part))
    .withColumn("next_age_avg_5", F.lead("age_avg_5").over(w_part))
    .withColumn("prev_segment_count_age_plus_credit_limit", F.lag("segment_count_age_plus_credit_limit").over(w_part))
    .withColumn("next_segment_count_age_plus_credit_limit", F.lead("segment_count_age_plus_credit_limit").over(w_part))
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("cumulative_segment_count_age_plus_credit_limit", F.sum("segment_count_age_plus_credit_limit").over(w6))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isNull()), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser").alias("session_browser"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser"), F.col("c.device").alias("session_device"))
)

totals = (
    with_latest
    .groupBy("session_browser", "browser")
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.round(F.sum("prev_segment_count_age_plus_credit_limit") / F.nullif(F.count(F.when(F.col("session_browser") == "Safari", 1)), F.lit(0)), 2).alias("prev_segment_count_age_plus_credit_limit_per_safari"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
)

result = (
    totals
    .select(
        "session_browser",
        "browser",
        "age_bit_or",
        "prev_segment_count_age_plus_credit_limit_per_safari",
        "consumer_count",
    )
    .orderBy(
        "session_browser",
        F.col("prev_segment_count_age_plus_credit_limit_per_safari").desc(),
    )
)
