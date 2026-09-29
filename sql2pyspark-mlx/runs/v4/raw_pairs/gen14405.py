from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("cost", "id", "promo_id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w4 = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id")
w5 = Window.partitionBy("status").orderBy("shipped_date", "id", "promo_id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-09-29").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.status"), F.col("d.channel"))
)

attributed = (
    with_dims
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.shipped_date") >= F.col("pr.starts_on")) & (F.col("s.shipped_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

deduped = (
    attributed
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w2)), 4))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("row_seq_2", F.row_number().over(w5))
)

valid_shipments = (
    with_windows
    .filter((F.col("shipped_date") > F.lit("2023-04-20").cast("date")) & F.col("promo_code").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "days_since_delivered_date",
        "status",
        "channel",
        "promo_id",
        "promo_code",
        "discount_pct",
        "latest_rank",
        "promo_id_share",
        "seq_no",
        "row_seq",
        "row_seq_2",
        (F.col("promo_id_share") * F.col("seq_no")).alias("promo_id_share_times_seq_no"),
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

result = (
    valid_shipments
    .cube(
        F.date_trunc("month", "shipped_date").alias("activity_month"),
        F.date_trunc("week", "shipped_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("row_seq_2"))).alias("bundle_total_row_seq_2"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
