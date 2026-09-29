from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < F.col("s.signup_date")), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.country",
        "s.city",
        "s.segment",
        "s.credit_limit",
        "s.age",
        F.col("c.end_date"),
        F.col("c.start_date").alias("subscription_start_date"),
    )
)

ranked = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "segment")
)

valid = (
    ranked
    .filter(F.col("segment").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "*",
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Group A").otherwise("Other").alias("segment_group"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.channel") == "partner"), "left_anti")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isNotNull()), "left_anti")
)

result = (
    eligible_customers
    .groupBy("segment")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
    )
    .orderBy("distinct_segments")
)
