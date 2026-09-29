from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.status").alias("account_status"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_semi")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") < F.col("s.signup_date")), "left_anti")
)

cleaned = (
    eligible_customers
    .filter(F.col("city").isNotNull() & F.col("email").isNull())
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "account_opened_date",
        "account_status",
        F.regexp_replace("name", " ", "").alias("name_slug"),
    )
)

filtered = (
    cleaned
    .filter(F.col("signup_date") > F.lit("2023-07-26").cast("date"))
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "age",
        "account_opened_date",
        "name_slug",
        F.date_format("account_opened_date", "MMM yyyy").alias("account_opened_date_label"),
    )
)

daily = (
    filtered
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.stddev_samp("age").alias("age_stddev"),
        F.stddev_samp("age").alias("age_stddev_2"),
    )
)

result = (
    daily
    .select("week_start", "cnt", "age_stddev", "age_stddev_2")
    .orderBy(F.col("week_start").desc())
)
