from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-07-21")))
    .select(
        "*",
        F.col("age").cast("double").alias("age_str"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.age_str",
        F.col("c.plan").alias("subscription_plan"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"), F.col("c.device"))
)

matched_keys = (
    latest_match
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
        .select("id", "segment")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

valid_customers = (
    kept_rows
    .filter(F.col("subscription_plan").isNotNull() & F.col("device").isNotNull())
    .select(
        "id",
        "name",
        "segment",
        "is_active",
        "age_str",
        "started_at",
        "device",
        F.dayofweek("signup_date").alias("signup_date_dayofweek"),
        F.initcap(F.trim("segment")).alias("segment_title"),
    )
)

result = (
    valid_customers
    .select(
        "name",
        "segment",
        "is_active",
        "age_str",
        "started_at",
        "device",
        "signup_date_dayofweek",
        "segment_title",
    )
)
