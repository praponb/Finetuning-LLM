from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
w2 = Window.partitionBy("store_id").orderBy("discount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w_hist = Window.partitionBy("store_id").orderBy("opened_date", "id")
w5 = Window.orderBy("sqft")

kept = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_semi")
)

latest = (
    kept
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

valid = (
    latest
    .filter((F.col("order_date") >= F.lit("2023-04-21").cast("date")) & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
        F.date_add("order_date", 84).alias("order_date_plus_84d"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.order_date",
        "a.amount",
        "a.discount_filled",
        "a.order_date_plus_84d",
        F.col("b.sqft"),
        F.col("b.opened_date"),
        F.col("b.store_name"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        "sqft",
        "opened_date",
        "store_name",
        F.max("discount_filled").over(w2).alias("running_max_discount_filled"),
        F.sum("discount_filled").over(w2).alias("running_sum_discount_filled"),
        F.avg("discount_filled").over(w2).alias("running_avg_discount_filled"),
        F.min("discount_filled").over(w2).alias("running_min_discount_filled"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

metrics_orders = (
    metrics
    .select(
        "id",
        "store_id",
        "order_date",
        "sqft",
        "running_max_discount_filled",
        "running_sum_discount_filled",
        "running_avg_discount_filled",
        "running_min_discount_filled",
        "amount_share",
        F.lead("running_sum_discount_filled").over(w_hist).alias("next_running_sum_discount_filled"),
        F.lag("running_sum_discount_filled").over(w_hist).alias("prev_running_sum_discount_filled"),
        F.lead("running_avg_discount_filled").over(w_hist).alias("next_running_avg_discount_filled"),
        F.lag("running_avg_discount_filled").over(w_hist).alias("prev_running_avg_discount_filled"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.percent_rank().over(w5).alias("sqft_percent_rank"),
    )
)

result = (
    metrics_orders
    .select(
        "id",
        "sqft",
        "running_max_discount_filled",
        "running_min_discount_filled",
        "amount_share",
        "next_running_sum_discount_filled",
        "next_running_avg_discount_filled",
        "prev_running_avg_discount_filled",
        "event_seq",
    )
)
