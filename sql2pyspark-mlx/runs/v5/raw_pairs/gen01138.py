from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("customer_id", "browser")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["US", "TH", "JP"]))
        .select("customer_id", "browser")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").rlike("^(Edge|Firefox)$"), "CORE").otherwise("EXTENDED").alias("browser_zone"),
    )
    .orderBy(F.col("started_at").desc_nulls_last())
)
