from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec").between(260, 441)), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country").rlike("^(US|SG)$"), "CORE").when(F.col("country").rlike("^(TH|JP)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("country_zone"),
    )
)
