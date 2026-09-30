from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("segment", "order_status").orderBy(F.col("age").desc())

base = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select(
        "*",
        F.when(F.col("city") == "Berlin", 1).otherwise(0).alias("is_berlin"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.start_date").alias("subscription_start_date"),
        F.col("c.status"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.is_berlin",
        "s.segment_clean",
        "s.subscription_start_date",
        F.col("c.status").alias("order_status"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

latest = (
    with_last
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("week", "subscription_start_date").alias("week_start"))
    .agg(
        F.avg("order_shipping_fee").alias("avg_order_shipping_fee"),
        F.bit_or("age").alias("age_bit_or"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.bit_or("age").alias("age_bit_or_2"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
