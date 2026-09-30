from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("account_balance", "id")
w3 = Window.orderBy(F.col("account_balance").asc_nulls_first())
w4 = Window.orderBy(F.col("account_balance").desc(), "id")
w_grp = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("account_status").orderBy(F.col("prev_account_balance").desc_nulls_first())
w8 = Window.partitionBy("account_status").orderBy(F.col("running_min_prev_account_balance_whole").desc(), "id")
w9 = Window.partitionBy("session_browser").orderBy("running_count_prev_account_balance_whole", "id")

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.signup_date", F.col("c.balance").alias("account_balance"))
)

scored = (
    with_last
    .filter(F.col("signup_date") >= "2023-06-30")
    .select(
        "id",
        "signup_date",
        "account_balance",
        F.lag("account_balance", 1).over(w2).alias("prev_account_balance"),
        F.percent_rank().over(w3).alias("account_balance_percent_rank"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

valid = (
    scored
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-04-12")))
    .withColumn("prev_account_balance_whole", F.floor("prev_account_balance"))
)

metrics = (
    valid
    .select(
        "*",
        F.avg("prev_account_balance_whole").over(w_grp).alias("running_avg_prev_account_balance_whole"),
        F.count("prev_account_balance_whole").over(w_grp).alias("running_count_prev_account_balance_whole"),
        F.min("prev_account_balance_whole").over(w_grp).alias("running_min_prev_account_balance_whole"),
        F.max("prev_account_balance_whole").over(w_grp).alias("running_max_prev_account_balance_whole"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.signup_date",
        "s.account_balance",
        "s.prev_account_balance",
        "s.event_seq",
        "s.prev_account_balance_whole",
        "s.running_avg_prev_account_balance_whole",
        "s.running_count_prev_account_balance_whole",
        "s.running_min_prev_account_balance_whole",
        F.col("c.status").alias("account_status"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser").alias("session_browser"))
)

valid_customers = (
    latest_match
    .filter((F.col("signup_date") >= F.lit("2023-03-12").cast("date")) & F.col("account_status").isNotNull())
    .select(
        "id",
        "signup_date",
        "account_balance",
        "prev_account_balance",
        "event_seq",
        "running_count_prev_account_balance_whole",
        "running_min_prev_account_balance_whole",
        "account_status",
        "session_browser",
        F.concat("session_browser", F.lit(" "), "account_status").alias("session_browser_account_status"),
        F.substring("session_browser", 1, 2).alias("session_browser_prefix"),
    )
)

result = (
    valid_customers
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("prev_account_balance_cume_dist"),
        F.ntile(3).over(w8).alias("running_min_prev_account_balance_whole_bucket"),
        F.ntile(4).over(w9).alias("running_count_prev_account_balance_whole_bucket"),
    )
)
