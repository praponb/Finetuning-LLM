from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-07-12"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "segment",
        "is_active",
        F.trim("city").alias("clean_city"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

filtered_customers = (
    filtered
    .filter(F.col("segment") == "SMB")
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

with_latest = (
    filtered_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.is_active",
        "s.clean_city",
        "s.is_active_label",
        F.col("c.start_date"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.balance"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < F.col("s.start_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("balance") >= 87000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.sum("balance").alias("total_balance"),
    )
)
