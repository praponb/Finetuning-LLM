from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc())
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())
w4 = Window.partitionBy("carrier")
w5 = Window.partitionBy("status", "carrier")
w6 = Window.partitionBy("order_id", "order_status").orderBy(F.col("running_shipping_fee").desc())

joined = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.shipping_fee"))
)

scored = (
    joined
    .filter(F.col("shipping_fee") > 24)
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w_part))
    .withColumn("cost_percent_rank", F.percent_rank().over(w3))
)

with_dims = (
    scored
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.delivered_date",
        "src.status",
        "src.running_shipping_fee",
        F.col("b.status").alias("order_status"),
        F.col("b.discount"),
    )
)

remaining_keys = (
    with_dims
    .select("id", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("cost") >= 76)
        .select("id", "carrier")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

scored_shipments = (
    kept_rows
    .select(
        "shipped_date",
        "status",
        "order_status",
        F.sum("running_shipping_fee").over(w4).alias("carrier_sum_running_shipping_fee"),
        F.stddev_samp("discount").over(w5).alias("status_stddev_samp_discount"),
        F.rank().over(w6).alias("running_shipping_fee_rank"),
    )
)

result = (
    scored_shipments
    .withColumn("running_shipping_fee_rank_tier", F.when(F.col("running_shipping_fee_rank") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("status_stddev_samp_discount_tier", F.when(F.col("status_stddev_samp_discount") >= 0, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_shipping_fee_rank_tier", "status_stddev_samp_discount_tier"], ["running_shipping_fee_rank_tier"]],
        "running_shipping_fee_rank_tier",
        "status_stddev_samp_discount_tier",
    )
    .agg(
        F.sort_array(F.collect_set("order_status")).alias("order_status_list"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.round(F.sum("carrier_sum_running_shipping_fee") / F.nullif(F.count(F.when(F.col("order_status") == "SHIPPED", 1)), F.lit(0)), 2).alias("carrier_sum_running_shipping_fee_per_shipped"),
        F.sum("carrier_sum_running_shipping_fee").alias("total_carrier_sum_running_shipping_fee"),
    )
    .filter(F.col("carrier_sum_running_shipping_fee_per_shipped") >= 0)
)
