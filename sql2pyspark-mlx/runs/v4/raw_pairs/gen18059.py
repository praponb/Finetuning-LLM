from pyspark.sql import functions as F

kept_keys = (
    df_subscriptions
    .select("id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("end_date") < "2024-01-01")
        .select("id")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("plan").rlike("^(premium|enterprise)$"), "GROUP_A").when(F.col("plan").rlike("^(basic|standard)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("plan_zone"),
    )
)
