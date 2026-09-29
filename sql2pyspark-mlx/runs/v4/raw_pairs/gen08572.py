from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel").alias("order_channel"))
)

filtered = (
    with_last
    .filter((F.col("signup_date") >= F.lit("2024-03-09").cast("date")) & F.col("credit_limit").isNull())
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "order_channel",
        F.upper("email").alias("email_upper"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") >= F.col("s.signup_date")), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("order_channel").isNotNull())
    .withColumn("segment_filled", F.coalesce("segment", F.lit("unassigned")))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("UNKNOWN")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.browser").isin(["Safari", "Chrome"])), "left_anti")
)

cleaned_customers = (
    not_flagged
    .filter(F.col("signup_date") > F.lit("2023-04-29").cast("date"))
    .select(
        "*",
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled_2"),
    )
)

cleaned_customers_2 = (
    cleaned_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "email_upper",
        "days_since_signup_date",
        "segment_clean",
        "signup_date_plus_months",
        F.col("age").cast("double").alias("age_str"),
        F.lower("order_channel").alias("order_channel_lower"),
    )
)

result = (
    cleaned_customers_2
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "days_since_signup_date",
        "segment_clean",
        "age_str",
        "order_channel_lower",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "EXTENDED").when(F.col("segment").rlike("^(Consumer)$"), "GROUP_B").otherwise("PRIORITY").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").desc())
)
