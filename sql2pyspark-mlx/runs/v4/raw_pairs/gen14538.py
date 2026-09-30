from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w2 = Window.partitionBy("status")

with_dims = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.credit_limit"), F.col("c.country"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.signup_date"), F.col("c.email"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

filtered = (
    with_windows
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        F.length("plan").alias("plan_length"),
        (F.col("prev_monthly_fee") - F.col("credit_limit")).alias("prev_monthly_fee_minus_credit_limit"),
    )
)

result = (
    filtered
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "credit_limit",
        "country",
        "signup_date",
        "prev_monthly_fee",
    )
    .orderBy("credit_limit")
)
