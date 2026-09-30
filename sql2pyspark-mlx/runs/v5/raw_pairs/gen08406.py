from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("country").eqNullSafe(F.lit("JP"))) & F.hour("started_at").between(9, 15))
    .select(
        "*",
        F.substring("device", 1, 3).alias("device_prefix"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

remaining_keys = (
    filtered
    .select("customer_id", "country")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.customer_id",
        "src.device",
        "src.started_at",
        "src.country",
        "src.device_prefix",
        F.col("c.credit_limit"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.customer_id",
        "s.device",
        "s.device_prefix",
        F.col("c.email"),
        F.col("c.segment"),
    )
)

result = (
    joined
    .groupBy("customer_id", "device")
    .agg(
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.count("*").alias("cnt"),
    )
)
