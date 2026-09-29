from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.orderBy("last7_max_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_avg_shipping_fee", "id")
w4 = Window.orderBy("running_count_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.discount"), F.col("p.shipping_fee"))
)

with_windows = (
    joined
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "discount",
        "shipping_fee",
        F.count("cost").over(w).alias("last7_count_cost"),
        F.max("cost").over(w).alias("last7_max_cost"),
        F.min("cost").over(w).alias("last7_min_cost"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "shipped_date",
        F.min("shipping_fee").over(w_running).alias("running_min_shipping_fee"),
        F.avg("shipping_fee").over(w_running).alias("running_avg_shipping_fee"),
        F.count("shipping_fee").over(w_running).alias("running_count_shipping_fee"),
        F.sum("shipping_fee").over(w_running).alias("running_sum_shipping_fee"),
    )
)

enriched = (
    metrics
    .filter(F.col("running_sum_shipping_fee") < 70)
    .withColumn("next_running_avg_shipping_fee", F.lead("running_avg_shipping_fee").over(w3))
    .withColumn("next_running_sum_shipping_fee", F.lead("running_sum_shipping_fee").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("last_known_running_sum_shipping_fee", F.last("running_sum_shipping_fee", ignorenulls=True).over(w4))
)

report = (
    enriched
    .select(
        "id",
        "shipped_date",
        "running_min_shipping_fee",
        "running_avg_shipping_fee",
        "running_count_shipping_fee",
        "running_sum_shipping_fee",
        "next_running_avg_shipping_fee",
        "next_running_sum_shipping_fee",
        "seq_no",
    )
)

result = (
    report
    .select(
        "id",
        "shipped_date",
        "running_min_shipping_fee",
        "running_avg_shipping_fee",
        "running_sum_shipping_fee",
        "next_running_avg_shipping_fee",
        "seq_no",
    )
    .orderBy("shipped_date")
)
