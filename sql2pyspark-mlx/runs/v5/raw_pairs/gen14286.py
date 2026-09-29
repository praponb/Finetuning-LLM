from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.city",
        "s.is_active",
        "s.age",
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
        F.col("c.status").alias("subscription_status"),
    )
)

with_latest_customers = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date").alias("subscription_start_date"))
)

not_flagged = (
    with_latest_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.subscription_start_date")), "left_anti")
)

with_latest_customers_2 = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.is_active",
        F.col("c.channel").alias("order_channel"),
    )
)

cleaned = (
    with_latest_customers_2
    .filter(F.col("order_channel") == "partner")
    .select(
        "id",
        "email",
        "country",
        "order_channel",
        F.upper(F.trim("email")).alias("email_normalized"),
    )
)

stats = (
    cleaned
    .groupBy("country")
    .agg(
        F.count_if(F.col("order_channel") == "mobile").alias("mobile_events"),
        F.count_if(F.col("order_channel") == "mobile").alias("mobile_events_2"),
        F.count_if(F.col("order_channel") == "web").alias("web_events"),
        F.countDistinct("order_channel").alias("distinct_order_channels"),
    )
)

result = (
    stats
    .select("country", "mobile_events", "distinct_order_channels")
    .orderBy(F.col("distinct_order_channels").desc(), "mobile_events")
)
