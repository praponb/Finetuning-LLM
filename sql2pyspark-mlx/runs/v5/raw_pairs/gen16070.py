from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

remaining_keys = (
    df_sessions
    .select("customer_id", "device")
    .subtract(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("customer_id", "device")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.when(F.col("browser").rlike("^(Chrome|Safari)$"), "EXTENDED").when(F.col("browser").rlike("^(Firefox|Edge)$"), "GROUP_B").otherwise("GROUP_A").alias("browser_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy("browser")
)
