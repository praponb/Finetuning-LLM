from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost_scaled")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy("order_date", "id")
w4 = Window.partitionBy("status").orderBy("status_stddev_samp_shipping_fee", "id")
w5 = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("delivered_date") > F.lit("2023-03-31").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
        F.add_months("shipped_date", 6).alias("shipped_date_plus_months"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.shipping_fee"), F.col("p.channel"))
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.delivered_date",
        "src.shipping_fee",
        F.col("p.status"),
        F.col("p.order_date"),
    )
)

scored = (
    joined
    .select(
        "id",
        "carrier",
        "delivered_date",
        "shipping_fee",
        "status",
        F.stddev_samp("shipping_fee").over(w2).alias("status_stddev_samp_shipping_fee"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_shipments = (
    scored
    .select(
        "id",
        "delivered_date",
        F.lag("status_stddev_samp_shipping_fee").over(w4).alias("prev_status_stddev_samp_shipping_fee"),
        F.lead("row_seq").over(w4).alias("next_row_seq"),
        F.lag("row_seq").over(w4).alias("prev_row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.last("shipping_fee", ignorenulls=True).over(w5).alias("last_known_shipping_fee"),
    )
)

result = (
    enriched_shipments
    .groupBy(F.date_trunc("quarter", "delivered_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq_2", 0.95).alias("row_seq_2_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("row_count").desc())
)
