from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-06-02"))) & F.col("name").like("K%"))
    .select("*", F.coalesce("segment", F.lit("N/A")).alias("segment_filled"))
)

matched_keys = (
    base
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.lower("name").like("%vy %"))
        .select("id", "segment")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("segment").isNotNull() & F.col("city").isin(["Singapore", "Bangkok", "New York"]))
    .select(
        "*",
        F.when(F.col("segment").isin(["Corporate", "SMB"]), "Group A").otherwise("Other").alias("segment_group"),
        F.initcap(F.trim("segment")).alias("segment_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.account_type").isin(["SAVINGS", "CREDIT", "CHECKING"])), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") < F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .select(
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "segment_group",
        "segment_title",
    )
)
