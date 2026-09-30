from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
    )
)

matched_keys = (
    with_windows
    .select("id", "status")
    .intersectAll(
        df_subscriptions
        .filter(F.col("customer_id").isin([1, 31, 36]))
        .select("id", "status")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("status", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee") < 150)
        .select("status", "plan")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.name"), F.col("p.signup_date"), F.col("p.country"))
)

enriched_subscriptions = (
    enriched
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.age"), F.col("b.segment"), F.col("b.city"))
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.segment") == "Corporate")
    .select("a.*", F.col("b.email"))
)

enriched_subscriptions_3 = (
    enriched_subscriptions_2
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.monthly_fee",
        "s.status",
        "s.monthly_fee_avg_6",
        "s.signup_date",
        "s.country",
        "s.city",
        "s.email",
        F.col("c.city").alias("customer_city"),
        F.col("c.signup_date").alias("customer_signup_date"),
        F.col("c.credit_limit"),
    )
)

result = (
    enriched_subscriptions_3
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "monthly_fee_avg_6",
        "signup_date",
        "city",
        "email",
        "customer_city",
        F.col("min_subscription_monthly_fee"),
    )
)
