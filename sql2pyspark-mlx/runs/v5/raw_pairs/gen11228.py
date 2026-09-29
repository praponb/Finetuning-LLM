from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_last(), "id")

valid = (
    df_customers
    .filter(F.length("email") >= 13)
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
        F.upper("country").alias("country_upper"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "country", "segment", "age", "segment_clean")
)

latest_match = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isNotNull()), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        "age",
        "segment_clean",
        "started_at",
        F.coalesce("segment", F.lit("unassigned")).alias("segment_filled"),
        F.substring("country", 1, 4).alias("country_prefix"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(F.when(F.col("age") >= 72, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
