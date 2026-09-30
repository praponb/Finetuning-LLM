from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser")
w_running = Window.partitionBy("city").orderBy("age", "id")
w4 = Window.partitionBy("session_country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("browser").orderBy(F.col("age").desc(), "id")
w6 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.age",
        F.col("c.browser"),
        F.col("c.country").alias("session_country"),
    )
)

scored = (
    with_last
    .select(
        "*",
        F.sum("age").over(w2).alias("browser_sum_age"),
        F.row_number().over(w_running).alias("seq_no"),
        F.first("session_country", ignorenulls=True).over(w4).alias("first_known_session_country"),
        F.ntile(5).over(w_recent).alias("age_bucket"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.browser",
        "s.session_country",
        "s.browser_sum_age",
        "s.first_known_session_country",
        F.col("c.status").alias("account_status"),
        F.col("c.balance"),
    )
)

grouped = (
    latest_match
    .groupBy(
        "browser",
        F.when(F.col("browser_sum_age") >= 249, "PREMIUM").otherwise("STANDARD").alias("browser_sum_age_tier"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") >= 1)
)

result = (
    grouped
    .select("browser", "browser_sum_age_tier", "total_balance")
    .orderBy(F.col("total_balance").asc_nulls_last())
)
