from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("browser").orderBy(F.col("signup_date").desc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")

latest = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    latest
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.year(F.col("c.signup_date")) >= 2024)
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.started_at",
        F.col("c.signup_date"),
        F.col("c.country").alias("customer_country"),
    )
)

latest_sessions = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "started_at")
)

not_flagged = (
    latest_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.to_date(F.col("x.event_time")) < "2023-07-13"), "left_anti")
)

base = (
    not_flagged
    .filter((F.col("started_at") > F.to_date(F.lit("2023-05-28"))) & F.col("customer_id").isNotNull())
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

grouped = (
    ranked
    .groupBy(F.date_trunc("year", "started_at").alias("year_start"))
    .agg(
        F.percentile_approx("days_since_started_at", [0.25, 0.5, 0.75]).alias("days_since_started_at_quartiles"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
)

result = (
    grouped
    .select(
        "year_start",
        "days_since_started_at_quartiles",
        "duration_min",
        "span_minutes",
    )
)
