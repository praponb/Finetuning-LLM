from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("opened_date", "id")
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("next_running_max_amount_rounded", "id")
w5 = Window.partitionBy("store_id").orderBy("event_seq", "id")
w6 = Window.partitionBy("store_id").orderBy("running_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id")
w8 = Window.orderBy(F.col("prev_sqft").desc(), "id")
w9 = Window.partitionBy("store_id").orderBy("running_avg_amount", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy("prev_sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("store_id").orderBy("next_first_known_running_avg_amount", "id")

enriched = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.min("amount").over(w).alias("running_min_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
    )
)

filtered = (
    enriched
    .filter((F.col("order_date") >= F.lit("2023-04-13").cast("date")) & (F.col("channel") != "store"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "discount",
        "shipping_fee",
        "running_min_amount",
        "running_sum_amount",
        "running_max_amount",
        "running_avg_amount",
        F.round("running_max_amount", 2).alias("running_max_amount_rounded"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.running_max_amount",
        "e.running_avg_amount",
        "e.running_max_amount_rounded",
        F.col("b.store_name"),
        F.col("b.opened_date"),
        F.col("b.sqft"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "store_id",
        "running_avg_amount",
        "running_max_amount_rounded",
        "store_name",
        F.lead("running_max_amount_rounded").over(w2).alias("next_running_max_amount_rounded"),
        F.lead("sqft").over(w2).alias("next_sqft"),
        F.lag("sqft").over(w2).alias("prev_sqft"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("running_avg_amount", ignorenulls=True).over(w3).alias("first_known_running_avg_amount"),
    )
)

with_windows = (
    metrics
    .filter(F.col("next_sqft").between(5438, 15612))
    .select(
        "id",
        "store_id",
        "running_avg_amount",
        "running_max_amount_rounded",
        "store_name",
        "next_running_max_amount_rounded",
        "prev_sqft",
        "event_seq",
        "first_known_running_avg_amount",
        F.lead("first_known_running_avg_amount", 1).over(w4).alias("next_first_known_running_avg_amount"),
        F.lag("next_sqft", 1).over(w5).alias("prev_next_sqft"),
        F.sum("running_avg_amount").over(w6).alias("cumulative_running_avg_amount"),
        F.round((F.col("first_known_running_avg_amount") / F.sum("first_known_running_avg_amount").over(w7)), 4).alias("first_known_running_avg_amount_share"),
        F.ntile(10).over(w8).alias("prev_sqft_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "running_avg_amount",
        "running_max_amount_rounded",
        "store_name",
        "next_running_max_amount_rounded",
        "prev_sqft",
        "first_known_running_avg_amount",
        "prev_next_sqft",
        "cumulative_running_avg_amount",
        "first_known_running_avg_amount_share",
        F.avg("cumulative_running_avg_amount").over(w9).alias("cumulative_running_avg_amount_avg_3"),
        F.sum("first_known_running_avg_amount").over(w10).alias("cumulative_first_known_running_avg_amount"),
        F.lag("first_known_running_avg_amount", 2).over(w11).alias("prev_first_known_running_avg_amount"),
    )
)
