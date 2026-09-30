from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.orderBy("row_num", "id")
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("shipped_date").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("discount").asc_nulls_last())
w7 = Window.partitionBy("channel", "carrier")
w8 = Window.partitionBy("order_id").orderBy("row_seq", "id")
w9 = Window.partitionBy("carrier", "channel").orderBy("discount_cume_dist", "id")

filtered = (
    df_shipments
    .filter(F.col("delivered_date") > F.to_date(F.lit("2024-02-22")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.discount"), F.col("b.channel"))
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("carrier_count_cost", F.count("cost").over(w3))
)

enriched = (
    with_windows
    .withColumn("last_known_carrier", F.last("carrier", ignorenulls=True).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("discount_cume_dist", F.cume_dist().over(w6))
)

metrics = (
    enriched
    .withColumn("channel_min_discount_cume_dist", F.min("discount_cume_dist").over(w7))
    .withColumn("event_seq_2", F.row_number().over(w8))
    .withColumn("seq_no", F.row_number().over(w9))
)

grouped = (
    metrics
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("activity_month"), "carrier")
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("carrier") == "Flash", 1)), F.lit(0)), 2).alias("seq_no_per_flash"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
    )
)

result = (
    grouped
    .select(
        "activity_month",
        "carrier",
        "seq_no_per_flash",
        F.when(F.col("carrier").rlike("^(UPS|Flash)$"), "CORE").otherwise("EXTENDED").alias("carrier_zone"),
    )
)
