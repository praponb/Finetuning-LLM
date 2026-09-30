from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")
w3 = Window.partitionBy("order_id").orderBy("cost", "id")
w4 = Window.orderBy(F.col("next_cost_2").desc(), "id")
w5 = Window.orderBy("order_id_stddev_samp_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("cost").desc(), "id")
w7 = Window.partitionBy("channel").orderBy("shipping_fee", "id")
w_recent = Window.partitionBy("channel").orderBy("next_cost_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("next_cost_2", "id")
w10 = Window.partitionBy("channel", "order_id").orderBy("next_cost_2_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-06-30").cast("date"))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

enriched = (
    filtered
    .withColumn("order_id_stddev_samp_cost", F.stddev_samp("cost").over(w))
    .withColumn("next_cost", F.lead("cost", 1).over(w2))
    .withColumn("next_cost_2", F.lead("cost", 1).over(w3))
)

enriched_shipments = (
    enriched
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.discount"), F.col("p.channel"), F.col("p.shipping_fee"))
)

metrics = (
    enriched_shipments
    .withColumn("next_cost_2_bucket", F.ntile(10).over(w4))
    .withColumn("cumulative_discount", F.sum("discount").over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

result = (
    metrics
    .withColumn("next_discount", F.lead("discount", 1).over(w7))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w_recent))
    .withColumn("next_order_id_stddev_samp_cost", F.lead("order_id_stddev_samp_cost", 2).over(w_grp))
    .withColumn("first_known_cumulative_discount", F.first("cumulative_discount", ignorenulls=True).over(w10))
    .withColumn("cost_bucket", F.ntile(4).over(w6))
)
