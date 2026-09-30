from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id")
w2 = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter(F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "*",
        (F.col("amount") / F.nullif("shipping_fee", F.lit(0))).alias("amount_per_shipping_fee"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "discount",
        F.lead("discount").over(w).alias("next_discount"),
        F.lag("discount").over(w).alias("prev_discount"),
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("event_seq"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "next_amount",
        "prev_amount",
        F.max("discount").over(w2).alias("running_max_discount"),
        F.min("discount").over(w2).alias("running_min_discount"),
        F.count("discount").over(w2).alias("running_count_discount"),
        F.avg("discount").over(w2).alias("running_avg_discount"),
        F.first("next_amount", ignorenulls=True).over(w3).alias("first_known_next_amount"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "FAILED"), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.city"), F.col("b.store_name"))
)

result = (
    with_dims
    .groupBy(
        "city",
        F.when(F.col("running_min_discount") >= 0.36, "LARGE").otherwise("SMALL").alias("running_min_discount_tier"),
    )
    .agg(
        F.round(F.avg("running_count_discount"), 2).alias("avg_running_count_discount"),
        F.max("amount").alias("max_amount"),
        F.max(F.when(F.col("status") == "COMPLETED", F.col("prev_amount"))).alias("completed_max_prev_amount"),
        F.percentile_approx("next_amount", 0.9).alias("next_amount_p90"),
    )
    .filter(F.col("completed_max_prev_amount") < 1000000)
    .orderBy(F.col("city").desc())
)
