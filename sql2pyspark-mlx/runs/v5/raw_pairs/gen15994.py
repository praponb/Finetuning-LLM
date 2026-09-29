from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first(), "id")
w2 = Window.partitionBy("segment", "city").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("segment", "city").orderBy("age", "id")
w4 = Window.partitionBy("credit_limit_tier").orderBy(F.col("event_count").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-07-07")))
    .select("*", (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.country").isin(["JP", "US"])), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (~F.col("ch.browser").isin(["Safari", "Firefox", "Edge"])), "left_semi")
)

metrics = (
    eligible
    .select(
        "id",
        "email",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age_plus_credit_limit",
        "rank_in_group",
        F.count("age_plus_credit_limit").over(w2).alias("last4_count_age_plus_credit_limit"),
        F.sum("age_plus_credit_limit").over(w2).alias("last4_sum_age_plus_credit_limit"),
        F.avg("age_plus_credit_limit").over(w2).alias("last4_avg_age_plus_credit_limit"),
        F.lead("age", 1).over(w3).alias("next_age"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("credit_limit") >= 14500, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
        F.when(F.col("last4_sum_age_plus_credit_limit") >= 410, "HIGH").otherwise("LOW").alias("last4_sum_age_plus_credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("next_age", 0.95).alias("next_age_p95"),
        F.round(F.sum("last4_avg_age_plus_credit_limit") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("last4_avg_age_plus_credit_limit_per_london"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
    )
    .withColumn("event_count_percent_rank", F.percent_rank().over(w4))
)

result = (
    grouped
    .select(
        "credit_limit_tier",
        "last4_sum_age_plus_credit_limit_tier",
        "last4_avg_age_plus_credit_limit_per_london",
        "event_count",
        "chiang_mai_events",
        "credit_limit_quartiles",
    )
    .orderBy(F.col("event_count").desc())
)
