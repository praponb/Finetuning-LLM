from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id", "channel").orderBy("discount", "id")
w3 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-03-29").cast("date"))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "channel_clean",
        F.corr("amount", "discount").over(w).alias("corr_amount_discount"),
        F.lag("discount", 1).over(w2).alias("prev_discount"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.avg("discount").over(w4).alias("discount_avg_5"),
        F.count("discount").over(w).alias("customer_id_count_discount"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.store_id",
        "a.order_date",
        "a.amount",
        "a.discount",
        "a.channel_clean",
        "a.corr_amount_discount",
        "a.discount_avg_5",
        "a.customer_id_count_discount",
        F.col("p.city"),
        F.col("p.name"),
    )
)

with_window = (
    with_dims
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.order_date") >= F.col("sub.start_date")) & (F.col("e.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.shipped_date") >= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "channel_clean",
        "corr_amount_discount",
        "discount_avg_5",
    )
)
