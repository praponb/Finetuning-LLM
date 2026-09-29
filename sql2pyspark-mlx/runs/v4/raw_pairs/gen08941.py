from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.signup_date"), F.col("d.email"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_6"),
    )
)

enriched_subscriptions = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.credit_limit"),
        F.col("p.signup_date").alias("customer_signup_date"),
    )
)

valid = (
    enriched_subscriptions
    .filter(F.col("start_date").between("2025-10-23", "2025-10-26"))
    .select("*", F.initcap(F.trim("email")).alias("email_title"))
)

result = (
    valid
    .select("id", "status", "monthly_fee", "monthly_fee_share", "credit_limit")
    .unpivot(
        ["id", "status"],
        ["monthly_fee", "monthly_fee_share", "credit_limit"],
        "metric",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
