from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("credit_limit", "id")
w_part = Window.partitionBy("segment")
w8 = Window.partitionBy("country")
w9 = Window.partitionBy("city").orderBy("age", "id")
w10 = Window.orderBy(F.col("first_known_credit_limit_whole").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2024-04-18")))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.floor("credit_limit").alias("credit_limit_whole"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") <= F.col("s.signup_date")), "left_semi")
)

latest = (
    kept
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country"))
)

with_windows = (
    with_latest
    .select(
        "*",
        F.sum("credit_limit_whole").over(w_grp).alias("cumulative_credit_limit_whole"),
        F.avg("age").over(w4).alias("age_avg_3"),
        F.first("credit_limit_whole", ignorenulls=True).over(w5).alias("first_known_credit_limit_whole"),
        F.ntile(10).over(w_running).alias("credit_limit_bucket"),
        F.avg("credit_limit_whole").over(w_part).alias("segment_avg_credit_limit_whole"),
    )
)

result = (
    with_windows
    .withColumn("country_stddev_samp_credit_limit_whole", F.stddev_samp("credit_limit_whole").over(w8))
    .withColumn("age_bucket", F.ntile(5).over(w9))
    .withColumn("event_seq", F.row_number().over(w10))
)
