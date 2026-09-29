from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "subscription_plan").orderBy("subscription_end_date", "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-13").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.concat("name", F.lit("/"), "city").alias("name_city"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.is_active_label",
        F.col("c.plan").alias("subscription_plan"),
        F.col("c.end_date").alias("subscription_end_date"),
    )
)

deduped = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < "2025-06-22"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "email",
        "is_active",
        "is_active_label",
        "subscription_plan",
        "subscription_end_date",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
