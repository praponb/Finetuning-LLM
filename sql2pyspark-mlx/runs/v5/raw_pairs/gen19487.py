from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy("credit_limit_times_age", "id")
w3 = Window.partitionBy("city").orderBy("credit_limit_times_age", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("city").orderBy(F.col("running_credit_limit").desc())
w7 = Window.partitionBy("city").orderBy("credit_limit_times_age", "id")
w8 = Window.partitionBy("country").orderBy("order_amount", "id")
w9 = Window.partitionBy("city", "country").orderBy("subscription_end_date", "id")
w10 = Window.orderBy("credit_limit_times_age", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("city").isNotNull() & F.lower("name").like("%ob %"))
    .withColumn("email_prefix", F.substring("email", 1, 3))
    .withColumn("country_prefix", F.substring("country", 1, 4))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

cleaned = (
    valid
    .filter(F.col("country") != "JP")
    .withColumn("country_lower", F.lower("country"))
    .withColumn("credit_limit_times_age", (F.col("credit_limit") * F.col("age")))
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
        "is_active",
        "credit_limit",
        "email_prefix",
        "country_prefix",
        "city_filled",
        "country_lower",
        "credit_limit_times_age",
        F.sum("credit_limit").over(w_ordered).alias("running_credit_limit"),
        F.lead("credit_limit", 1).over(w2).alias("next_credit_limit"),
        F.avg("age").over(w3).alias("age_avg_5"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.email_prefix",
        "s.country_prefix",
        "s.credit_limit_times_age",
        "s.running_credit_limit",
        F.col("c.amount").alias("order_amount"),
    )
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.email_prefix",
        "s.country_prefix",
        "s.credit_limit_times_age",
        "s.running_credit_limit",
        "s.order_amount",
        F.col("c.end_date").alias("subscription_end_date"),
    )
)

not_flagged = (
    with_last_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.subscription_end_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "country",
        "city",
        "order_amount",
        "subscription_end_date",
        F.percent_rank().over(w6).alias("running_credit_limit_percent_rank"),
        F.ntile(4).over(w7).alias("credit_limit_times_age_bucket"),
        F.lead("credit_limit_times_age", 1).over(w8).alias("next_credit_limit_times_age"),
        F.row_number().over(w9).alias("seq_no"),
        F.avg("order_amount").over(w10).alias("order_amount_avg_6"),
    )
)

result = (
    scored
    .select(
        "id",
        "country",
        "running_credit_limit_percent_rank",
        "next_credit_limit_times_age",
        "order_amount",
    )
    .unpivot(
        ids=["id", "country"],
        values=["running_credit_limit_percent_rank", "next_credit_limit_times_age", "order_amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
