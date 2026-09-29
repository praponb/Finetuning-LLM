from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id", "key")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first(), "id", "key", "subscription_id")
w3 = Window.partitionBy("plan").orderBy("event_seq", "id", "key", "subscription_id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("event_seq", "id", "key", "subscription_id")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

scored = (
    exploded
    .filter(F.col("key") == "utm_source")
    .select("*", F.row_number().over(w).alias("event_seq"))
)

matched = (
    scored
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.created_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

ranked = (
    matched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .withColumn("last4_count_event_seq", F.count("event_seq").over(w3))
    .withColumn("last4_avg_event_seq", F.avg("event_seq").over(w3))
    .withColumn("last4_sum_event_seq", F.sum("event_seq").over(w3))
    .withColumn("last4_min_event_seq", F.min("event_seq").over(w3))
    .withColumn("next_subscription_id", F.lead("subscription_id", 2).over(w4))
)

result = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.country").alias("country"), F.col("e.key").alias("key"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.last4_sum_event_seq")).alias("max_last4_sum_event_seq"),
    )
    .orderBy(F.col("num_rows").desc_nulls_first())
)
