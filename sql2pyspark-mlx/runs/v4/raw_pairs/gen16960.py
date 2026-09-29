from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.concat_ws("-", "browser", "country").alias("browser_country_key"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

matched_keys = (
    valid
    .select("customer_id", "country")
    .subtract(
        df_sessions
        .filter(F.col("browser") == "Chrome")
        .select("customer_id", "country")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("country").isNotNull() & (F.col("started_at") < "2024-09-27"))
    .select(
        "*",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.age"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.country").alias("customer_country"),
        F.col("d.email"),
        F.col("d.age").alias("customer_age"),
    )
)

result = (
    with_dims
    .groupBy("customer_id", "customer_country")
    .pivot("country", ["DE", "UK"])
    .agg(
        F.sum("customer_age").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
