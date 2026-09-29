from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy(F.col("discount").desc(), "id", "subscription_id")
w_running = Window.orderBy("discount", "id", "subscription_id")
w_part = Window.orderBy(F.col("order_date").desc(), "id", "subscription_id")

matched = (
    df_orders
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    matched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isNull()), "left_semi")
)

metrics = (
    kept
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("seq_no", F.row_number().over(w_part))
)

result = (
    metrics
    .withColumn("subscription_id_tier", F.when(F.col("subscription_id") >= 8, "PREMIUM").otherwise("STANDARD"))
    .rollup("subscription_id_tier", "subscription_id")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("cnt"),
        F.min("amount").alias("min_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("discount_per_premium"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("discount_per_premium") < 1000000)
    .orderBy(F.col("subscription_id_tier").asc_nulls_last())
)
