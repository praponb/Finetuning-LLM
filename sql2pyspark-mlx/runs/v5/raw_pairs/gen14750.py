from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_status", "channel").orderBy(F.col("order_date").desc(), "id")
w5 = Window.orderBy(F.col("order_date").desc(), "id")
w6 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("carrier").orderBy("seq_no", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("last4_count_seq_no").orderBy(F.col("distinct_carriers").desc())

filtered = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "DELIVERED", "LOST"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.trim("status").alias("clean_status"),
    )
)

with_windows = (
    filtered
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w))
    .withColumn("running_cost", F.sum("cost").over(w_hist))
    .withColumn("order_id_count_cost", F.count("cost").over(w3))
)

joined = (
    with_windows
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isin(["web", "store", "partner"]))
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.status",
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
        F.col("o.order_date"),
    )
)

with_windows_shipments = (
    joined
    .select(
        "id",
        "carrier",
        "shipped_date",
        "order_date",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("seq_no"),
        F.last("order_status", ignorenulls=True).over(w6).alias("last_known_order_status"),
    )
)

with_windows_shipments_2 = (
    with_windows_shipments
    .select(
        "*",
        F.count("seq_no").over(w7).alias("last4_count_seq_no"),
        F.avg("seq_no").over(w7).alias("last4_avg_seq_no"),
        F.min("seq_no").over(w7).alias("last4_min_seq_no"),
        F.max("seq_no").over(w7).alias("last4_max_seq_no"),
    )
)

result = (
    with_windows_shipments_2
    .groupBy(
        "last4_count_seq_no",
        F.when(F.col("last4_count_seq_no") >= 15, "HIGH").otherwise("LOW").alias("last4_count_seq_no_tier"),
    )
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.sum("last4_max_seq_no").alias("total_last4_max_seq_no"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_rows"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
    )
    .withColumn("distinct_carriers_dense_rank", F.dense_rank().over(w8))
)
