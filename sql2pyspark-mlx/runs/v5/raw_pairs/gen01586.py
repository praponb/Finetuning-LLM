from pyspark.sql import functions as F

matched_keys = (
    df_subscriptions
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") >= 220)
        .select("id")
    )
)

kept_rows = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
    )
    .orderBy(F.col("plan").asc_nulls_last(), F.col("end_date").desc())
)
