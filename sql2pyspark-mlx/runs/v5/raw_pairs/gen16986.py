from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date_period", "id")
w2 = Window.partitionBy("status").orderBy("delivered_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("delivered_date_period", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("last4_max_burst_id", "id")
w6 = Window.orderBy("burst_id", "id")
w7 = Window.orderBy("last4_avg_burst_id", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy("last4_avg_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last4_sum_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("next_burst_id").desc(), "id")
w_part = Window.orderBy("next_burst_id", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("delivered_date_period") - F.lag("delivered_date_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_shipments = (
    flagged
    .select(
        "id",
        "delivered_date",
        "status",
        "delivered_date_period",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

with_windows = (
    sessions_shipments
    .select(
        "id",
        "burst_id",
        F.sum("burst_id").over(w_hist).alias("last4_sum_burst_id"),
        F.avg("burst_id").over(w_hist).alias("last4_avg_burst_id"),
        F.max("burst_id").over(w_hist).alias("last4_max_burst_id"),
        F.avg("burst_id").over(w4).alias("burst_id_avg_6_prior"),
    )
)

enriched = (
    with_windows
    .filter(F.col("last4_max_burst_id") >= 3.5)
    .select(
        "id",
        "last4_sum_burst_id",
        "last4_avg_burst_id",
        "burst_id_avg_6_prior",
        F.lead("burst_id", 1).over(w5).alias("next_burst_id"),
        F.lead("last4_max_burst_id", 1).over(w6).alias("next_last4_max_burst_id"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "last4_sum_burst_id",
        "last4_avg_burst_id",
        "burst_id_avg_6_prior",
        "next_burst_id",
        "next_last4_max_burst_id",
        F.avg("burst_id_avg_6_prior").over(w7).alias("burst_id_avg_6_prior_avg_5"),
        F.last("burst_id_avg_6_prior", ignorenulls=True).over(w_running).alias("last_known_burst_id_avg_6_prior"),
        F.last("next_burst_id", ignorenulls=True).over(w9).alias("last_known_next_burst_id"),
    )
)

with_windows_shipments = (
    metrics
    .select(
        "id",
        "last4_avg_burst_id",
        "next_burst_id",
        "next_last4_max_burst_id",
        F.ntile(10).over(w10).alias("next_burst_id_bucket"),
        F.avg("next_last4_max_burst_id").over(w_part).alias("next_last4_max_burst_id_avg_5"),
    )
)

base = (
    with_windows_shipments
    .filter(F.col("next_burst_id_bucket") < 6)
    .select(
        "id",
        "last4_avg_burst_id",
        "next_burst_id",
        "next_last4_max_burst_id",
        "next_last4_max_burst_id_avg_5",
        F.coalesce("next_burst_id_bucket", F.lit(0)).alias("next_burst_id_bucket_filled"),
        F.coalesce("next_burst_id", F.lit(0)).alias("next_burst_id_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("next_burst_id") >= 7, "LARGE").otherwise("SMALL").alias("next_burst_id_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("next_burst_id_filled"), 2).alias("avg_next_burst_id_filled"),
        F.round(F.avg("next_burst_id_filled"), 2).alias("avg_next_burst_id_filled_2"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("avg_next_burst_id_filled_2").desc_nulls_last())
)
