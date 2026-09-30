from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-23").cast("date")) & F.col("city").isNull())
    .select("*", F.length("country").alias("country_length"))
)

enriched = (
    cleaned
    .filter(F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "email",
        "country",
        "is_active",
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.lead("country_length").over(w).alias("next_country_length"),
        F.row_number().over(w).alias("row_seq"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.is_active",
        "s.next_credit_limit",
        F.col("c.country").alias("session_country"),
        F.col("c.browser").alias("session_browser"),
    )
)

with_latest_customers = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.session_country",
        "s.session_browser",
        F.col("c.start_date"),
        F.col("c.end_date").alias("subscription_end_date"),
    )
)

summary = (
    with_latest_customers
    .groupBy("country")
    .agg(
        F.size(F.collect_set("session_browser")).alias("num_session_browsers"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.count(F.when(F.col("session_country") == "SG", 1)).alias("sg_count"),
        F.count(F.when(F.col("session_browser") == "Firefox", 1)).alias("firefox_rows"),
        F.size(F.collect_set("session_browser")).alias("num_session_browsers_2"),
    )
    .filter(F.col("sg_count") >= 1)
)

result = summary.select("country", "sg_count", "firefox_rows", "num_session_browsers_2")
