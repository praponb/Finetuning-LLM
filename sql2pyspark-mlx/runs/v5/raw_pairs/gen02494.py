from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w3 = Window.orderBy(F.col("account_balance_2").desc_nulls_last())
w4 = Window.partitionBy("account_status", "country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("country").orderBy("account_opened_date", "id")
w6 = Window.partitionBy("account_status", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.signup_date",
        "s.credit_limit",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.balance").alias("account_balance"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.balance").alias("account_balance_2"),
        F.col("c.status").alias("account_status"),
    )
)

metrics = (
    with_last
    .select(
        "id",
        "email",
        "country",
        "credit_limit",
        "account_opened_date",
        "account_balance",
        F.cume_dist().over(w3).alias("account_balance_2_cume_dist"),
        F.row_number().over(w4).alias("seq_no"),
        F.lag("account_balance_2", 1).over(w5).alias("prev_account_balance_2"),
        F.last("account_status", ignorenulls=True).over(w6).alias("last_known_account_status"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") <= F.col("s.account_opened_date")), "left_anti")
)

with_last_customers = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.credit_limit",
        "s.account_opened_date",
        "s.account_balance_2_cume_dist",
        "s.seq_no",
        "s.prev_account_balance_2",
        F.col("c.country").alias("session_country"),
    )
)

eligible = (
    with_last_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") < "2025-10-24"), "left_semi")
)

report = (
    eligible
    .select("id", "credit_limit", "seq_no", "session_country")
    .orderBy("seq_no", "session_country")
)

result = (
    report
    .select(
        "id",
        "credit_limit",
        "seq_no",
        "session_country",
        F.when(F.col("session_country").rlike("^(DE|UK)$"), "EXTENDED").when(F.col("session_country").rlike("^(TH|US)$"), "CORE").otherwise("OTHER_ZONE").alias("session_country_zone"),
    )
    .orderBy(F.col("credit_limit").asc_nulls_first(), F.col("id").desc())
)
