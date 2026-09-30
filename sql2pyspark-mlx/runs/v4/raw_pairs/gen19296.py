from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("device", "id")
    .subtract(
        df_sessions
        .filter(F.col("started_at") < "2025-03-01")
        .select("device", "id")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("started_at") >= F.lit("2023-10-03 07:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

valid = (
    base
    .filter(F.col("browser").isNotNull() & (F.col("customer_id") == 12))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "started_at_day",
        F.date_trunc("WEEK", "started_at").alias("started_at_period"),
        F.add_months("started_at_day", 6).alias("started_at_day_plus_months"),
    )
)

result = (
    valid
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("tablet_count").asc_nulls_last())
)
