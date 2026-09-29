from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w3 = Window.orderBy(F.col("running_avg_age").desc(), "id")
w4 = Window.orderBy("event_seq_rounded", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("shipping_fee", "id")

scored = (
    df_customers
    .select(
        "id",
        "name",
        "is_active",
        F.min("age").over(w_hist).alias("running_min_age"),
        F.sum("age").over(w_hist).alias("running_sum_age"),
        F.count("age").over(w_hist).alias("running_count_age"),
        F.avg("age").over(w_hist).alias("running_avg_age"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

base = (
    scored
    .filter(F.col("running_count_age") >= 5)
    .select(
        "id",
        "name",
        "is_active",
        "running_sum_age",
        "running_avg_age",
        "event_seq",
        F.round("event_seq", 0).alias("event_seq_rounded"),
        F.coalesce("name", F.lit("Unknown")).alias("name_filled"),
    )
)

base_customers = (
    base
    .filter(F.col("running_avg_age") > 59)
    .select(
        "id",
        "name",
        "is_active",
        "running_avg_age",
        "event_seq",
        "event_seq_rounded",
        "name_filled",
        (F.col("running_sum_age") - F.col("event_seq_rounded")).alias("running_sum_age_minus_event_seq_rounded"),
        F.initcap(F.trim("name")).alias("name_title"),
    )
)

valid = (
    base_customers
    .filter(F.col("running_sum_age_minus_event_seq_rounded") >= 24)
    .select(
        "id",
        "name",
        "is_active",
        "running_avg_age",
        "event_seq",
        "event_seq_rounded",
        "name_filled",
        "name_title",
        (F.col("running_sum_age_minus_event_seq_rounded") + F.col("running_avg_age")).alias("running_sum_age_minus_event_seq_rounded_plus_running_avg_age"),
    )
)

with_windows = (
    valid
    .withColumn("running_avg_age_bucket", F.ntile(10).over(w3))
    .withColumn("event_seq_2", F.row_number().over(w4))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.event_seq_rounded",
        "s.name_filled",
        "s.name_title",
        "s.event_seq_2",
        F.col("c.order_date"),
        F.col("c.shipping_fee"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "name",
        "name_filled",
        "name_title",
        "shipping_fee",
        F.row_number().over(w4).alias("event_seq"),
        F.last("event_seq_rounded", ignorenulls=True).over(w6).alias("last_known_event_seq_rounded"),
        F.row_number().over(w4).alias("seq_no"),
        F.lead("event_seq_rounded", 1).over(w7).alias("next_event_seq_rounded"),
    )
)
