from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("shipping_fee", "id")
w4 = Window.orderBy("first_known_shipping_fee", "id")
w5 = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_first(), "id")
w6 = Window.partitionBy("carrier", "status").orderBy(F.col("first_known_shipping_fee").desc(), "id")
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("order_id", "carrier")
w9 = Window.partitionBy("status").orderBy("shipping_fee_bucket", "id")
w10 = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.shipping_fee"), F.col("b.discount"))
)

with_windows = (
    joined
    .withColumn("cumulative_discount", F.sum("discount").over(w))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w2))
)

metrics = (
    with_windows
    .withColumn("shipping_fee_bucket", F.ntile(3).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

with_windows_shipments = (
    ranked
    .select(
        "*",
        F.count("event_seq").over(w7).alias("status_count_event_seq"),
        F.avg("discount").over(w8).alias("order_id_avg_discount"),
        F.ntile(10).over(w9).alias("shipping_fee_bucket_bucket"),
    )
)

ranked_shipments = (
    with_windows_shipments
    .withColumn("rank_in_group_2", F.row_number().over(w10))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    ranked_shipments
    .groupBy(F.date_trunc("YEAR", "shipped_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("event_seq_per_kerry"),
    )
    .filter(F.col("event_seq_per_kerry") > 0)
    .orderBy(F.col("event_seq_per_kerry").desc())
)
