from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-19").cast("date")) & F.col("city").isNotNull())
    .withColumn("days_since_signup_date", F.datediff(F.current_date(), "signup_date"))
    .withColumn("signup_date_period", F.date_trunc("year", "signup_date"))
)

kept = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date_period")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.email", "s.country", "s.city", "s.segment", F.col("c.channel"))
)

filtered = (
    with_last
    .filter(F.col("country").isin(["DE", "JP", "US"]))
    .select(
        "id",
        "email",
        "country",
        "channel",
        F.when(F.col("city").isin(["Berlin", "New York"]), "Tier 1").otherwise("Other").alias("city_group"),
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

valid = (
    filtered
    .filter(~F.col("country").isin(["DE", "SG"]))
    .select(
        "id",
        "email",
        "country",
        "city_group",
        "channel_title",
        F.lower("country").alias("country_lower"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = valid.select("id", "email", "city_group", "country_lower", "channel_clean")
