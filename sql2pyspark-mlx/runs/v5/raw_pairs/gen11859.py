from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("order_status", "channel").orderBy("next_cost", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("cost_share").desc(), "id")
w5 = Window.partitionBy("order_status").orderBy(F.col("next_cost").asc_nulls_first())
w_part = Window.partitionBy("status").orderBy(F.col("event_seq").desc(), "id", "promo_id")
w7 = Window.partitionBy("channel", "order_status").orderBy("row_seq", "id", "promo_id")
w8 = Window.partitionBy("promo_code").orderBy("last4_count_next_cost", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_status").orderBy("shipped_date", "id", "promo_id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isNotNull())
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.delivered_date",
        "src.cost",
        "src.status",
        "src.carrier_clean",
        F.col("d.status").alias("order_status"),
        F.col("d.channel"),
        F.col("d.order_date"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        "carrier_clean",
        "order_status",
        "channel",
        "order_date",
        F.lead("cost").over(w_running).alias("next_cost"),
        F.row_number().over(w_running).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

with_windows = (
    scored
    .withColumn("last4_count_next_cost", F.count("next_cost").over(w3))
    .withColumn("last4_sum_next_cost", F.sum("next_cost").over(w3))
    .withColumn("last4_avg_next_cost", F.avg("next_cost").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

attributed = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    attributed
    .select(
        "id",
        "shipped_date",
        "cost",
        "carrier_clean",
        "order_status",
        "last4_count_next_cost",
        "last4_sum_next_cost",
        "last4_avg_next_cost",
        "row_seq",
        "promo_id",
        "promo_code",
        F.dense_rank().over(w5).alias("next_cost_dense_rank"),
        F.ntile(10).over(w_part).alias("event_seq_bucket"),
        F.sum("cost_share").over(w2).alias("status_sum_cost_share"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

scored_shipments = (
    metrics
    .select(
        "*",
        F.sum("cost").over(w8).alias("running_cost"),
        F.avg("last4_sum_next_cost").over(w9).alias("last4_sum_next_cost_avg_3"),
    )
)

result = (
    scored_shipments
    .groupBy(F.date_trunc("YEAR", "shipped_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_count"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.avg("last4_avg_next_cost").alias("avg_last4_avg_next_cost"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.round(F.sum("last4_count_next_cost") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("last4_count_next_cost_per_vip"),
    )
    .filter(F.col("avg_seq_no") > 0)
)
