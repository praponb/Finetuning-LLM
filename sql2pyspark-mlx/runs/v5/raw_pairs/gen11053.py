from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("age", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("segment").orderBy(F.col("city_stddev_samp_credit_limit").desc_nulls_last())
w5 = Window.partitionBy("segment")
w6 = Window.orderBy("city_stddev_samp_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_customers
    .select(
        "*",
        F.lag("credit_limit").over(w_grp).alias("prev_credit_limit"),
        F.lead("age").over(w_grp).alias("next_age"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.stddev_samp("credit_limit").over(w2).alias("city_stddev_samp_credit_limit"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < "2023-04-01"), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

filtered = (
    with_last
    .filter((F.col("segment").eqNullSafe(F.lit("SMB"))) & ~F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "prev_credit_limit",
        "event_seq",
        "city_stddev_samp_credit_limit",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

metrics = (
    filtered
    .filter(F.col("city_stddev_samp_credit_limit").isNull())
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "credit_limit",
        "prev_credit_limit",
        "event_seq",
        "city_stddev_samp_credit_limit",
        "status_prefix",
        F.rank().over(w_running).alias("city_stddev_samp_credit_limit_rank"),
        F.max("prev_credit_limit").over(w5).alias("segment_max_prev_credit_limit"),
        F.sum("credit_limit").over(w6).alias("running_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w5)), 4).alias("credit_limit_share"),
    )
)

valid = (
    metrics
    .filter(F.col("segment").eqNullSafe(F.lit("Consumer")))
    .withColumn("segment_upper", F.upper("segment"))
    .withColumn("name_lower", F.lower("name"))
)

result = (
    valid
    .groupBy(
        "segment",
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
    )
    .agg(
        F.percentile_approx("prev_credit_limit", [0.25, 0.5, 0.75]).alias("prev_credit_limit_quartiles"),
        F.round(F.sum("running_credit_limit") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("running_credit_limit_per_corporate"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("running_credit_limit")).otherwise(0)).alias("corporate_running_credit_limit"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
