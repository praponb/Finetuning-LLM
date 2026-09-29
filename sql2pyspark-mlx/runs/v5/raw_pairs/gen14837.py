from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-07-21"))) & F.col("browser").isNotNull())
    .select("*", F.initcap(F.trim("browser")).alias("browser_title"))
)

matched_keys = (
    valid
    .select("browser", "customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("browser") != "Safari")
        .select("browser", "customer_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec").between(348, 370)), "left_semi")
)

with_dims = (
    kept
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.age"), F.col("d.name"))
)

valid_sessions = (
    with_dims
    .filter(F.col("country").isNotNull())
    .withColumn("country_filled", F.coalesce("country", F.lit("N/A")))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Chrome", "Safari"]), "Core").otherwise("Other"))
    .withColumn("country_filled_2", F.coalesce("country", F.lit("DIRECT")))
)

with_dims_sessions = (
    valid_sessions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.*",
        F.col("b.name").alias("customer_name"),
        F.col("b.country").alias("customer_country"),
    )
)

result = (
    with_dims_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_title",
        "country_filled",
        "browser_group",
        "country_filled_2",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
