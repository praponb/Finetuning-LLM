from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.age"), F.col("p.city"))
)

enriched = (
    joined
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.segment"), F.col("b.name"), F.col("b.signup_date"))
)

with_window = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.signup_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

latest = (
    with_window
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched_subscriptions = (
    latest
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.credit_limit"), F.col("c.city").alias("customer_city"))
)

valid = (
    enriched_subscriptions
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "city",
        "segment",
        "name",
        "signup_date",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        "rn",
        "credit_limit",
        "customer_city",
        F.upper("city").alias("city_upper"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("quarter", "signup_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("customer_city")).alias("num_customer_cities"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.count(F.when(F.col("subscription_plan") == "premium", 1)).alias("premium_rows"),
    )
    .orderBy(F.col("quarter_start").desc())
)
