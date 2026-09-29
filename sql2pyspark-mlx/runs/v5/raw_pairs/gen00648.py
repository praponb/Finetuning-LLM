from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc(), "id")

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") <= 380), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.avg("amount").over(w).alias("last3_avg_amount"),
        F.max("amount").over(w).alias("last3_max_amount"),
        F.min("amount").over(w).alias("last3_min_amount"),
        F.sum("amount").over(w).alias("last3_sum_amount"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_window = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date") >= F.col("sub.start_date")) & (F.col("a.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

joined = (
    with_window
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("e.*", F.col("d.signup_date"), F.col("d.country"), F.col("d.credit_limit"))
)

result = (
    joined
    .groupBy(F.date_trunc("week", "order_date").alias("week_start"), "customer_id")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("plan") == "basic", F.col("last3_max_amount")).otherwise(0)).alias("basic_last3_max_amount"),
        F.round(F.avg("last3_avg_amount"), 2).alias("avg_last3_avg_amount"),
        F.countDistinct("status").alias("distinct_status"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
    )
    .filter(F.col("seq_no_p95") >= 0)
    .orderBy(F.col("corr_discount_amount").desc())
)
