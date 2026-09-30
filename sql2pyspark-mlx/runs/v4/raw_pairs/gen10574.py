from pyspark.sql import functions as F

attributed = (
    df_sessions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "subscription_id",
        "monthly_fee",
        F.when(F.col("country").rlike("^(TH|DE)$"), "EXTENDED").when(F.col("country").rlike("^(US|SG)$"), "GROUP_B").otherwise("GROUP_A").alias("country_zone"),
    )
)
