from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select("*", F.round((F.col("age") * 1.07), 2).alias("age_adjusted"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "age",
        "age_adjusted",
        F.min("age_adjusted").over(w_recent).alias("running_min_age_adjusted"),
        F.max("age_adjusted").over(w_recent).alias("running_max_age_adjusted"),
        F.avg("age_adjusted").over(w_recent).alias("running_avg_age_adjusted"),
        F.sum("age").over(w2).alias("cumulative_age"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("subscription_status"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.signup_date",
        "s.age",
        "s.cumulative_age",
        F.col("c.device").alias("session_device"),
        F.col("c.browser").alias("session_browser"),
    )
)

result = (
    with_latest
    .rollup(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("city") == "New York", F.col("cumulative_age")).otherwise(0)).alias("new_york_cumulative_age"),
        F.percentile_approx("age", 0.5).alias("age_p50_2"),
        F.count(F.when(F.col("session_browser") == "Chrome", 1)).alias("chrome_rows"),
        F.avg("cumulative_age").alias("avg_cumulative_age"),
    )
    .filter(F.col("age_p50") < 1000000)
    .orderBy(F.col("quarter_start").desc())
)
