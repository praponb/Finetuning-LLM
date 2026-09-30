from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy("signup_date", "id")
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("signup_date", "id")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("session_device")
w7 = Window.partitionBy("session_country")
w_running = Window.partitionBy("session_country", "city").orderBy(F.col("credit_limit_filled").desc_nulls_last())
w9 = Window.orderBy(F.col("event_seq").desc())
w10 = Window.partitionBy("session_device").orderBy(F.col("running_age").desc())

valid = (
    df_customers
    .filter(F.length("email") >= 9)
    .select(
        "*",
        F.upper("city").alias("city_upper"),
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device").alias("session_device"), F.col("c.browser"))
)

scored = (
    latest_match
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("running_age", F.sum("age").over(w3))
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w4))
    .withColumn("country_count_age", F.count("age").over(w5))
)

matched_keys = (
    scored
    .select("city")
    .exceptAll(
        df_customers
        .filter(F.col("age").between(52, 66))
        .select("city")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("segment", "id")
    .exceptAll(
        df_customers
        .filter(~F.col("country").isin(["UK", "SG", "JP"]))
        .select("segment", "id")
    )
)

reconciled_customers = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored_customers = (
    reconciled_customers
    .select(
        "*",
        F.max("age").over(w6).alias("session_device_max_age"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w7)), 4).alias("event_seq_share"),
        F.percent_rank().over(w_running).alias("credit_limit_filled_percent_rank"),
        F.rank().over(w9).alias("event_seq_rank"),
        F.rank().over(w10).alias("running_age_rank"),
    )
)

result = (
    scored_customers
    .groupBy("session_country")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.max("event_seq"))
)
