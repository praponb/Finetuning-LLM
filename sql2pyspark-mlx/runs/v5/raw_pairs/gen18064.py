from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.age") >= 63)
    .select(
        "src.id",
        "src.customer_id",
        "src.started_at",
        "src.country",
        F.col("d.age"),
        F.col("d.email"),
        F.col("d.signup_date"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "started_at",
        "country",
        "age",
        "email",
        "signup_date",
        F.lag("age").over(w_running).alias("prev_age"),
        F.lead("age").over(w_running).alias("next_age"),
        F.row_number().over(w_running).alias("row_seq"),
        F.first("age", ignorenulls=True).over(w2).alias("first_known_age"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.signup_date")), "left_anti")
)

base = (
    kept
    .filter(F.col("country").isNotNull() & (F.col("country").eqNullSafe(F.lit("SG"))))
    .withColumn("next_age_str", F.col("next_age").cast("double"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("DIRECT")))
)

prev_events = base.withColumn("prev_started_at", F.lag("started_at").over(w3))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w4))

result = (
    sessionized
    .cube(F.date_trunc("MONTH", "started_at").alias("activity_month"), "country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
        F.max(F.when(F.col("country") == "DE", F.col("row_seq"))).alias("de_max_row_seq"),
        F.avg("row_seq").alias("avg_row_seq"),
    )
)
