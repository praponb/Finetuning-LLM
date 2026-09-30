from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-11-09"))) & F.col("method").isNotNull())
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("row_count"),
        F.percentile_approx("status_length", 0.5).alias("status_length_p50"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("year_start").desc())
)
